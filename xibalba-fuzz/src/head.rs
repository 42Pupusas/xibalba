use xibalba_proto::error::{Error, ParseError};
use xibalba_proto::header::Header;
use xibalba_proto::response::ResponseHead;

/// Properties of [`ResponseHead::parse`] that must hold for arbitrary bytes.
pub struct HeadInvariants<'a> {
    data: &'a [u8],
}

impl<'a> HeadInvariants<'a> {
    /// Header slots offered to the parser. Small enough that `TooManyHeaders`
    /// stays reachable — a buffer no input can overflow would leave that
    /// branch untested.
    const SLOTS: usize = 16;

    /// The prefix sweep is quadratic, so it is capped rather than skipped:
    /// truncation defects live in the first bytes of a head, and an unbounded
    /// sweep would spend the whole fuzzing budget on one long input.
    const MAX_PREFIX_SWEEP: usize = 512;

    #[must_use]
    pub const fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn check_all(&self) {
        let mut headers = [const { Header::empty() }; Self::SLOTS];
        let parsed = ResponseHead::parse(self.data, &mut headers);

        match parsed {
            Ok((head, consumed)) => {
                self.consumed_is_within_the_buffer(consumed);
                Self::count_matches_the_buffer_it_describes(&head, &headers);
                self.parsed_bytes_are_borrowed_not_synthesised(&head, &headers);
                Self::no_field_smuggles_a_line_ending(&head, &headers);
                self.every_shorter_prefix_asks_for_more(consumed);
            }
            Err(e) => Self::a_failure_is_a_classification_not_a_crash(&e),
        }

        self.parsing_twice_agrees();
    }

    /// A consumed count past the end of the input would make a caller advance
    /// its buffer past bytes it never received.
    fn consumed_is_within_the_buffer(&self, consumed: usize) {
        assert!(
            consumed <= self.data.len(),
            "parse consumed {consumed} of a {}-byte buffer",
            self.data.len()
        );
    }

    /// The count and the header buffer are separate values, so an inconsistent
    /// pair is representable. `headers()` is the accessor that rules it out.
    fn count_matches_the_buffer_it_describes(head: &ResponseHead<'_>, slots: &[Header<'_>]) {
        assert!(
            head.header_count <= slots.len(),
            "reported {} headers into {} slots",
            head.header_count,
            slots.len()
        );
        let listed = head
            .headers(slots)
            .expect("a count within the buffer must yield its headers");
        assert_eq!(listed.len(), head.header_count);
    }

    /// The crate's central claim is that parsed types borrow the caller's
    /// buffer. A field pointing outside it would mean the parser fabricated or
    /// copied bytes, and the lifetime would no longer describe the data.
    fn parsed_bytes_are_borrowed_not_synthesised(
        &self,
        head: &ResponseHead<'_>,
        slots: &[Header<'_>],
    ) {
        assert!(
            self.is_borrowed_from_input(head.reason),
            "the reason phrase does not point into the input buffer"
        );
        for header in head.headers(slots).unwrap_or(&[]) {
            assert!(
                self.is_borrowed_from_input(header.name.as_bytes()),
                "a header name does not point into the input buffer"
            );
            assert!(
                self.is_borrowed_from_input(header.value),
                "a header value does not point into the input buffer"
            );
        }
    }

    /// A CR or LF surviving inside a parsed field is how a response gets
    /// re-serialised into two. The parser rejects them; this holds it to that
    /// for inputs nobody wrote by hand.
    fn no_field_smuggles_a_line_ending(head: &ResponseHead<'_>, slots: &[Header<'_>]) {
        assert!(
            !Self::has_line_ending(head.reason),
            "a parsed reason phrase contains CR or LF"
        );
        for header in head.headers(slots).unwrap_or(&[]) {
            assert!(
                !Self::has_line_ending(header.name.as_bytes()),
                "a parsed header name contains CR or LF"
            );
            assert!(
                !Self::has_line_ending(header.value),
                "a parsed header value contains CR or LF"
            );
        }
    }

    /// The property behind incremental reads: a caller feeding bytes as they
    /// arrive treats `Incomplete` as the only instruction to read more. If a
    /// truncated *prefix of a head that does parse* reported any other error,
    /// that caller would reject a response still in flight.
    fn every_shorter_prefix_asks_for_more(&self, consumed: usize) {
        let sweep = consumed.min(Self::MAX_PREFIX_SWEEP);
        for end in 1..sweep {
            let mut headers = [const { Header::empty() }; Self::SLOTS];
            let err = ResponseHead::parse(&self.data[..end], &mut headers).expect_err(
                "a prefix shorter than the parsed head cannot itself be a complete head",
            );
            assert_eq!(
                err,
                Error::Parse(ParseError::Incomplete),
                "the {end}-byte prefix of a head that parses at {consumed} reported {err:?}, \
                 which tells an incremental caller to give up rather than read on"
            );
        }
    }

    /// Failure must be a verdict the caller can act on. `Incomplete` in
    /// particular means "read more", so it may not be returned for input that
    /// no continuation could complete — but every variant is a classification,
    /// and the defect this rules out is the parser resolving to something it
    /// cannot name.
    fn a_failure_is_a_classification_not_a_crash(error: &Error) {
        assert!(
            matches!(error, Error::Parse(_)),
            "parsing bytes produced {error:?}, which is not a parse verdict"
        );
    }

    /// The parser holds no state between calls, so the same bytes must give
    /// the same verdict. A difference would mean it reads uninitialised slots
    /// or depends on what the buffer happened to contain.
    fn parsing_twice_agrees(&self) {
        let mut first_slots = [const { Header::empty() }; Self::SLOTS];
        let mut second_slots = [const { Header::empty() }; Self::SLOTS];
        let first = ResponseHead::parse(self.data, &mut first_slots);
        let second = ResponseHead::parse(self.data, &mut second_slots);

        match (first, second) {
            (Ok((a, a_used)), Ok((b, b_used))) => {
                assert_eq!(a.status, b.status, "status differed between identical parses");
                assert_eq!(a.version, b.version);
                assert_eq!(a.reason, b.reason);
                assert_eq!(a.header_count, b.header_count);
                assert_eq!(a_used, b_used);
                assert_eq!(
                    first_slots[..a.header_count],
                    second_slots[..b.header_count],
                    "the same bytes parsed to different headers"
                );
            }
            (Err(a), Err(b)) => assert_eq!(a, b, "the same bytes produced different errors"),
            (a, b) => panic!("parsing the same bytes twice disagreed: {a:?} then {b:?}"),
        }
    }

    fn is_borrowed_from_input(&self, slice: &[u8]) -> bool {
        if slice.is_empty() {
            return true;
        }
        let outer = self.data.as_ptr_range();
        let inner = slice.as_ptr_range();
        inner.start >= outer.start && inner.end <= outer.end
    }

    fn has_line_ending(slice: &[u8]) -> bool {
        slice.iter().any(|&b| b == b'\r' || b == b'\n')
    }
}
