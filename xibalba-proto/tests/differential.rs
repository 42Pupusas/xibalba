//! Cross-checks this parser against `httparse` on the same bytes.
//!
//! A parser is only as good as its disagreements with other parsers. Where two
//! implementations read the same response differently, one of them is what an
//! attacker uses to smuggle a request past the other, so the interesting
//! output of this file is not "both accept" but "they diverge, and here is why
//! that is allowed".
//!
//! `httparse` is a reference, not an authority. It is a deliberately lenient
//! syntax parser: it does not decide framing, does not resolve
//! Transfer-Encoding against Content-Length, and accepts several constructs
//! RFC 9112 tells a *recipient* to reject. So a divergence is judged against
//! the RFC, and the expectation is recorded in the case itself rather than
//! being whatever the other parser happened to do.

use xibalba_proto::error::{Error, ParseError};
use xibalba_proto::header::Header;
use xibalba_proto::response::{ChunkedDecoder, DecodeResult, MAX_HEADERS, ResponseHead};

/// How the two parsers are expected to relate on one input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Agreement {
    /// Both accept, and the status/headers this parser produces match
    /// `httparse` field for field.
    Both,
    /// Both reject. Error *kinds* are not compared: the taxonomies differ by
    /// design and matching them would test naming, not behaviour.
    Neither,
    /// `httparse` accepts and this parser refuses, because RFC 9112 puts the
    /// requirement on the recipient rather than on the syntax.
    OnlyHttparse,
}

/// A response head reduced to what both parsers agree to expose, so the two
/// can be compared at all: status code, and headers lowercased for the
/// case-insensitive comparison RFC 9110 §5.1 requires.
#[derive(Debug, PartialEq, Eq)]
struct HeadView {
    status: u16,
    headers: Vec<(String, Vec<u8>)>,
}

/// One response head, and what the two parsers should make of it.
struct HeadCase {
    name: &'static str,
    input: &'static [u8],
    expect: Agreement,
    /// Why a divergence is correct. Required for [`Agreement::OnlyHttparse`],
    /// so no asymmetry can be written down without a reason for it.
    rationale: &'static str,
}

impl HeadCase {
    const fn agree(name: &'static str, input: &'static [u8]) -> Self {
        Self {
            name,
            input,
            expect: Agreement::Both,
            rationale: "",
        }
    }

    const fn both_reject(name: &'static str, input: &'static [u8]) -> Self {
        Self {
            name,
            input,
            expect: Agreement::Neither,
            rationale: "",
        }
    }

    const fn stricter(name: &'static str, input: &'static [u8], rationale: &'static str) -> Self {
        Self {
            name,
            input,
            expect: Agreement::OnlyHttparse,
            rationale,
        }
    }

    /// What this crate's parser makes of the input.
    fn ours(&self) -> Result<HeadView, Error> {
        let mut headers = [const { Header::empty() }; MAX_HEADERS];
        let (head, _) = ResponseHead::parse(self.input, &mut headers)?;
        let parsed = head
            .headers(&headers)
            .expect("a parsed head describes its own buffer");
        Ok(HeadView {
            status: head.status.as_u16(),
            headers: parsed
                .iter()
                .map(|h| {
                    (
                        String::from_utf8_lossy(h.name.as_bytes()).to_lowercase(),
                        h.value.to_vec(),
                    )
                })
                .collect(),
        })
    }

    /// What `httparse` makes of the same bytes.
    fn theirs(&self) -> Result<Option<HeadView>, httparse::Error> {
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut response = httparse::Response::new(&mut headers);
        match response.parse(self.input)? {
            httparse::Status::Partial => Ok(None),
            httparse::Status::Complete(_) => Ok(Some(HeadView {
                status: response.code.expect("a complete parse has a status"),
                headers: response
                    .headers
                    .iter()
                    .map(|h| (h.name.to_lowercase(), h.value.to_vec()))
                    .collect(),
            })),
        }
    }

    fn check(&self) {
        let ours = self.ours();
        let theirs = self.theirs();

        match self.expect {
            Agreement::Both => {
                let ours = ours.unwrap_or_else(|e| {
                    panic!(
                        "{}: this parser rejected what httparse accepts: {e:?}",
                        self.name
                    )
                });
                let theirs = theirs
                    .unwrap_or_else(|e| panic!("{}: httparse rejected: {e:?}", self.name))
                    .unwrap_or_else(|| panic!("{}: httparse wanted more input", self.name));
                assert_eq!(
                    ours, theirs,
                    "{}: both parsers accepted but read the response differently, \
                     which is the shape a smuggling divergence takes",
                    self.name
                );
            }
            Agreement::Neither => {
                assert!(
                    ours.is_err(),
                    "{}: this parser accepted input httparse rejects",
                    self.name
                );
                assert!(
                    matches!(theirs, Err(_) | Ok(None)),
                    "{}: httparse accepted input this parser rejects, without a \
                     recorded rationale",
                    self.name
                );
            }
            Agreement::OnlyHttparse => {
                assert!(
                    !self.rationale.is_empty(),
                    "{}: an asymmetry needs a reason",
                    self.name
                );
                assert!(
                    ours.is_err(),
                    "{}: expected this parser to be the stricter one ({}), but it accepted",
                    self.name,
                    self.rationale
                );
                assert!(
                    matches!(theirs, Ok(Some(_))),
                    "{}: httparse was expected to accept this ({}); if it now \
                     rejects too, the case is no longer an asymmetry and should \
                     become both_reject",
                    self.name,
                    self.rationale
                );
            }
        }
    }
}

/// The corpus. Ordinary heads first, then the constructs that decide whether
/// two recipients read the same message.
struct HeadCorpus;

impl HeadCorpus {
    const CASES: &'static [HeadCase] = &[
        HeadCase::agree("plain 200", b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n"),
        HeadCase::agree("no headers", b"HTTP/1.1 204 No Content\r\n\r\n"),
        HeadCase::agree(
            "absent reason phrase",
            b"HTTP/1.1 200 \r\nContent-Length: 0\r\n\r\n",
        ),
        HeadCase::agree(
            "value whitespace is trimmed",
            b"HTTP/1.1 200 OK\r\nContent-Length:   3   \r\n\r\n",
        ),
        HeadCase::agree(
            "empty header value",
            b"HTTP/1.1 200 OK\r\nX-Empty:\r\nContent-Length: 0\r\n\r\n",
        ),
        HeadCase::agree(
            "duplicate names are kept in order",
            b"HTTP/1.1 200 OK\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\n\r\n",
        ),
        HeadCase::agree(
            "mixed-case names",
            b"HTTP/1.1 200 OK\r\nCoNtEnT-lEnGtH: 3\r\n\r\n",
        ),
        HeadCase::agree(
            "both framings present, syntax is still valid",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n",
        ),
        HeadCase::agree("http/1.0", b"HTTP/1.0 200 OK\r\nContent-Length: 3\r\n\r\n"),
        HeadCase::both_reject("truncated head", b"HTTP/1.1 200 OK\r\nContent-Len"),
        HeadCase::both_reject("garbage status line", b"NOT-HTTP 200 OK\r\n\r\n"),
        HeadCase::both_reject(
            "space before the colon",
            b"HTTP/1.1 200 OK\r\nContent-Length : 3\r\n\r\n",
        ),
        HeadCase::both_reject(
            "nul in a header name",
            b"HTTP/1.1 200 OK\r\nX-\0-Bad: 1\r\n\r\n",
        ),
        HeadCase::both_reject("empty header name", b"HTTP/1.1 200 OK\r\n: novalue\r\n\r\n"),
        HeadCase::stricter(
            "bare LF between headers",
            b"HTTP/1.1 200 OK\nContent-Length: 3\r\n\r\n",
            "RFC 9112 2.2 permits a recipient to accept a bare LF as a line \
             terminator, but a proxy that requires CRLF would see a different \
             message; refusing keeps the framing single-valued",
        ),
        // Predicted as an asymmetry and measured as agreement: httparse
        // refuses obs-fold in its default configuration too (it is opt-in via
        // ParserConfig). RFC 9112 5.2 deprecates the construct outside
        // message/http, so both refusing is the outcome the RFC wants, and the
        // case stays as the regression that would catch either parser starting
        // to join continuation lines.
        HeadCase::both_reject(
            "obsolete line folding",
            b"HTTP/1.1 200 OK\r\nX-Long: one\r\n two\r\nContent-Length: 0\r\n\r\n",
        ),
    ];

    fn run() {
        for case in Self::CASES {
            case.check();
        }
    }
}

#[test]
fn response_heads_agree_with_httparse_or_diverge_for_a_recorded_reason() {
    HeadCorpus::run();
}

/// The manifest, read as text, so the dependency-free contract is checked
/// rather than remembered.
struct Manifest {
    text: String,
}

impl Manifest {
    fn load() -> Self {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        Self {
            text: std::fs::read_to_string(path).expect("read xibalba-proto/Cargo.toml"),
        }
    }

    /// The lines of the `[dependencies]` table, excluding dev and build tables.
    fn runtime_dependency_lines(&self) -> Vec<&str> {
        let mut lines = Vec::new();
        let mut in_runtime_deps = false;
        for line in self.text.lines() {
            let line = line.trim();
            if line.starts_with('[') {
                in_runtime_deps = line == "[dependencies]";
                continue;
            }
            if in_runtime_deps && !line.is_empty() && !line.starts_with('#') {
                lines.push(line);
            }
        }
        lines
    }
}

/// The reference parser is a test tool, and must stay one. A `[dependencies]`
/// entry here would put httparse into every consumer of the crate, which is
/// the opposite of what a zero-dependency protocol library is for.
#[test]
fn the_reference_parser_never_becomes_a_runtime_dependency() {
    let manifest = Manifest::load();
    assert!(
        manifest.runtime_dependency_lines().is_empty(),
        "xibalba-proto gained a runtime dependency: {:?}",
        manifest.runtime_dependency_lines()
    );
    assert!(
        manifest.text.contains("[dev-dependencies]"),
        "httparse must stay under dev-dependencies"
    );
}

/// A guard on the corpus itself: an asymmetry with no rationale, or a case
/// list that quietly shrinks, would weaken the suite without failing it.
#[test]
fn every_asymmetry_carries_a_rationale() {
    for case in HeadCorpus::CASES {
        if case.expect == Agreement::OnlyHttparse {
            assert!(
                !case.rationale.is_empty(),
                "{}: divergence without a reason",
                case.name
            );
        }
    }
    assert!(
        HeadCorpus::CASES.len() >= 16,
        "the differential corpus lost cases"
    );
}

/// One chunked body, decoded by both implementations.
///
/// `httparse` exposes only the chunk-*size* parser, not a whole-body decoder,
/// so the comparison is at that boundary: for each chunk header, both must
/// agree on the size and on how many bytes it occupied. That is precisely the
/// field a smuggling attack manipulates.
struct ChunkCase {
    name: &'static str,
    input: &'static [u8],
    /// The decoded body, or `None` when the stream must be rejected.
    expect: Option<&'static [u8]>,
}

impl ChunkCase {
    /// Decode with this crate's decoder, feeding the whole input at once.
    fn ours(&self) -> Result<Vec<u8>, ParseError> {
        let mut decoder = ChunkedDecoder::new();
        let mut body = Vec::new();
        let mut input = self.input;
        loop {
            let mut out = [0u8; 256];
            let (result, consumed) = decoder.decode(input, &mut out);
            input = &input[consumed..];
            match result {
                DecodeResult::Data(n) => {
                    body.extend_from_slice(&out[..n]);
                    if decoder.is_done() {
                        return Ok(body);
                    }
                }
                DecodeResult::Done => return Ok(body),
                DecodeResult::Error(e) => return Err(e),
                DecodeResult::NeedMore => {
                    if input.is_empty() {
                        return Err(ParseError::Incomplete);
                    }
                }
            }
        }
    }

    /// What `httparse` reads as the first chunk size.
    fn their_first_size(&self) -> Result<Option<(usize, u64)>, httparse::InvalidChunkSize> {
        match httparse::parse_chunk_size(self.input)? {
            httparse::Status::Complete(pair) => Ok(Some(pair)),
            httparse::Status::Partial => Ok(None),
        }
    }

    fn check(&self) {
        let ours = self.ours();
        match self.expect {
            Some(expected) => {
                let decoded = ours.unwrap_or_else(|e| {
                    panic!("{}: expected a clean decode, got {e:?}", self.name)
                });
                assert_eq!(
                    decoded, expected,
                    "{}: decoded body differs from the expectation",
                    self.name
                );
                assert!(
                    matches!(self.their_first_size(), Ok(Some(_))),
                    "{}: httparse could not read the first chunk size of a stream \
                     this parser decoded cleanly",
                    self.name
                );
            }
            None => assert!(
                ours.is_err(),
                "{}: an invalid chunked stream decoded without error",
                self.name
            ),
        }
    }
}

struct ChunkCorpus;

impl ChunkCorpus {
    const CASES: &'static [ChunkCase] = &[
        ChunkCase {
            name: "single chunk",
            input: b"4\r\nRust\r\n0\r\n\r\n",
            expect: Some(b"Rust"),
        },
        ChunkCase {
            name: "several chunks",
            input: b"3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n",
            expect: Some(b"abcdef"),
        },
        ChunkCase {
            name: "uppercase hex size",
            input: b"A\r\n0123456789\r\n0\r\n\r\n",
            expect: Some(b"0123456789"),
        },
        ChunkCase {
            name: "leading zeros in the size",
            input: b"0004\r\nRust\r\n0\r\n\r\n",
            expect: Some(b"Rust"),
        },
        ChunkCase {
            name: "chunk extension is ignored",
            input: b"4;name=value\r\nRust\r\n0\r\n\r\n",
            expect: Some(b"Rust"),
        },
        ChunkCase {
            name: "empty body",
            input: b"0\r\n\r\n",
            expect: Some(b""),
        },
        ChunkCase {
            name: "trailer after the last chunk",
            input: b"4\r\nRust\r\n0\r\nX-Check: 1\r\n\r\n",
            expect: Some(b"Rust"),
        },
        ChunkCase {
            name: "non-hex size",
            input: b"z\r\nRust\r\n0\r\n\r\n",
            expect: None,
        },
        ChunkCase {
            name: "size overflowing u64",
            input: b"FFFFFFFFFFFFFFFFF\r\nx\r\n0\r\n\r\n",
            expect: None,
        },
        ChunkCase {
            name: "empty size line",
            input: b"\r\nRust\r\n0\r\n\r\n",
            expect: None,
        },
    ];

    fn run() {
        for case in Self::CASES {
            case.check();
        }
    }
}

#[test]
fn chunked_bodies_decode_as_httparse_sizes_them() {
    ChunkCorpus::run();
}

/// The size field both implementations must read identically. A stream whose
/// declared size differs between two recipients is the classic smuggling
/// primitive, so this compares the number rather than the decoded bytes.
#[test]
fn chunk_sizes_are_read_identically() {
    for case in ChunkCorpus::CASES {
        let Ok(Some((_, their_size))) = case.their_first_size() else {
            continue;
        };
        let mut decoder = ChunkedDecoder::new();
        let mut out = [0u8; 256];
        let (result, _) = decoder.decode(case.input, &mut out);
        if let DecodeResult::Data(n) = result {
            assert_eq!(
                n as u64, their_size,
                "{}: this parser took {n} body bytes from the first chunk where \
                 httparse sized it at {their_size}",
                case.name
            );
        }
    }
}
