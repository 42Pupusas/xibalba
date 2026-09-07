use xibalba_proto::response::{ChunkedDecoder, DecodeResult};

/// Properties of [`ChunkedDecoder`] that must hold for arbitrary bytes.
pub struct ChunkedInvariants<'a> {
    data: &'a [u8],
}

/// What a decode run produced: the body bytes, and how it ended.
#[derive(Debug, PartialEq, Eq)]
struct Decoded {
    body: Vec<u8>,
    outcome: Outcome,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Complete,
    Errored,
    /// Input ran out mid-stream. Legitimate: the body is still arriving.
    Exhausted,
    /// The decoder stopped making progress on input it still had. A defect —
    /// the caller's read loop would spin here.
    Stalled,
}

impl<'a> ChunkedInvariants<'a> {
    const OUTPUT: usize = 256;

    /// A decoder that never reaches `Done` or an error must still stop. The
    /// bound is generous: it only has to be smaller than an infinite loop,
    /// which is the defect being ruled out.
    const MAX_STEPS: usize = 100_000;

    #[must_use]
    pub const fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn check_all(&self) {
        let whole = self.decode_in_slices(&[self.data.len().max(1)]);
        Self::a_run_terminates(&whole);
        self.split_feeds_agree_with_one_feed(&whole);
        self.a_tiny_output_buffer_loses_nothing(&whole);
    }

    /// The decoder is fed by a caller reading from a socket, so it sees the
    /// stream cut at arbitrary points. Where the cuts fall is a property of
    /// the network, and it must not change a single decoded byte.
    fn split_feeds_agree_with_one_feed(&self, whole: &Decoded) {
        for chunk in [1usize, 2, 3, 7, 13] {
            let split = self.decode_in_slices(&[chunk]);
            assert_eq!(
                split, *whole,
                "feeding the same stream in {chunk}-byte slices decoded differently \
                 than feeding it whole; where a socket read happens to break is not \
                 something the peer chose"
            );
        }

        let uneven = self.decode_in_slices(&[1, 5, 2, 11, 3]);
        assert_eq!(
            uneven, *whole,
            "feeding the same stream in unevenly sized slices decoded differently \
             than feeding it whole"
        );
    }

    /// The output buffer belongs to the caller and may be small. A decoder
    /// that only preserves bytes when the buffer is roomy would silently drop
    /// body data under memory pressure.
    fn a_tiny_output_buffer_loses_nothing(&self, whole: &Decoded) {
        let cramped = self.decode_with_output(self.data.len().max(1), 1);
        assert_eq!(
            cramped.body, whole.body,
            "decoding into a one-byte output buffer produced a different body"
        );
    }

    /// Progress or a verdict, never neither: a decoder that consumes nothing
    /// and reports `NeedMore` on input it still holds would spin forever in
    /// the caller's read loop. Running out of input is not that — a truncated
    /// body is a normal thing to receive.
    fn a_run_terminates(run: &Decoded) {
        assert_ne!(
            run.outcome,
            Outcome::Stalled,
            "the decoder stopped consuming input without finishing or failing"
        );
    }

    fn decode_in_slices(&self, pattern: &[usize]) -> Decoded {
        self.run(pattern, Self::OUTPUT)
    }

    fn decode_with_output(&self, slice: usize, output: usize) -> Decoded {
        self.run(&[slice], output)
    }

    /// Feed the input in slices sized by `pattern`, cycling through it, and
    /// collect everything the decoder emits.
    fn run(&self, pattern: &[usize], output_len: usize) -> Decoded {
        let mut decoder = ChunkedDecoder::new();
        let mut output = vec![0u8; output_len];
        let mut body = Vec::new();
        let mut fed = 0;
        let mut sizes = pattern.iter().copied().cycle();
        let mut steps = 0;

        while fed < self.data.len() {
            let want = sizes.next().unwrap_or(1).max(1);
            let end = fed.saturating_add(want).min(self.data.len());
            let mut window = &self.data[fed..end];

            // A slice is offered repeatedly until the decoder stops taking
            // bytes from it: one call may stop early to hand back a full
            // output buffer, and those bytes are not consumed yet.
            loop {
                steps += 1;
                if steps > Self::MAX_STEPS {
                    return Decoded {
                        body,
                        outcome: Outcome::Stalled,
                    };
                }

                let (result, consumed) = decoder.decode(window, &mut output);
                assert!(
                    consumed <= window.len(),
                    "the decoder consumed {consumed} bytes of a {}-byte slice",
                    window.len()
                );

                match result {
                    DecodeResult::Data(n) => {
                        assert!(
                            n <= output.len(),
                            "the decoder reported {n} bytes into a {}-byte output buffer",
                            output.len()
                        );
                        body.extend_from_slice(&output[..n]);
                    }
                    DecodeResult::Done => {
                        assert!(
                            decoder.is_done(),
                            "the decoder reported Done while is_done() says otherwise"
                        );
                        return Decoded {
                            body,
                            outcome: Outcome::Complete,
                        };
                    }
                    DecodeResult::Error(_) => {
                        return Decoded {
                            body,
                            outcome: Outcome::Errored,
                        };
                    }
                    DecodeResult::NeedMore => {}
                }

                window = &window[consumed..];
                fed += consumed;

                if window.is_empty() {
                    break;
                }
                if consumed == 0 && result == DecodeResult::NeedMore {
                    return Decoded {
                        body,
                        outcome: Outcome::Stalled,
                    };
                }
            }
        }

        Decoded {
            body,
            outcome: if decoder.is_done() {
                Outcome::Complete
            } else {
                Outcome::Exhausted
            },
        }
    }
}
