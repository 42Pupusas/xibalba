use crate::error::ParseError;
use crate::header::Tchar;

/// Largest chunk extension, in bytes, accepted on a single chunk.
pub const MAX_CHUNK_EXTENSION: usize = 4096;

/// Largest trailer section, in bytes, accepted after the final chunk.
pub const MAX_TRAILER_SECTION: usize = 8192;

/// Largest chunk-size line, in hex digits, accepted before the terminating
/// `;` or CRLF.
///
/// `u64::MAX` in hex is 16 digits, so digits past a small multiple of that
/// are always leading zeros: they cannot change the decoded value, only
/// consume input. Without a bound a peer can send zero digits forever
/// without ever reaching a `checked_mul` overflow, since the accumulator
/// stays at zero — the size-overflow check the digit loop already has does
/// not reach this case at all. Kept comfortably above the existing
/// leading-zero regression coverage (up to 512 zeros) rather than at the
/// tightest bound that would still pass it.
pub const MAX_CHUNK_SIZE_DIGITS: usize = 1024;

/// Bounds the metadata a peer may send between body bytes.
///
/// Extensions, trailers, and size-line digits carry no payload, so a peer
/// that streams them indefinitely keeps a request occupied without ever
/// making progress. Each budget is charged per byte and refuses the stream
/// once exhausted.
#[derive(Debug, Clone)]
struct MetadataBudget {
    size_digits: usize,
    extension: usize,
    trailer: usize,
}

impl MetadataBudget {
    const fn new() -> Self {
        Self {
            size_digits: 0,
            extension: 0,
            trailer: 0,
        }
    }

    /// Charge one size-line hex digit. Reset per chunk alongside the
    /// extension budget, since each chunk states its own size.
    const fn charge_size_digit(&mut self) -> bool {
        self.size_digits += 1;
        self.size_digits <= MAX_CHUNK_SIZE_DIGITS
    }

    /// Charge one extension byte. Reset per chunk, since each chunk is
    /// allowed its own extension.
    const fn charge_extension(&mut self) -> bool {
        self.extension += 1;
        self.extension <= MAX_CHUNK_EXTENSION
    }

    /// Charge one trailer byte. Not reset: the whole trailer section shares
    /// one budget, so many small trailer lines cannot evade it.
    const fn charge_trailer(&mut self) -> bool {
        self.trailer += 1;
        self.trailer <= MAX_TRAILER_SECTION
    }

    const fn reset_size_digits(&mut self) {
        self.size_digits = 0;
    }

    const fn reset_extension(&mut self) {
        self.extension = 0;
    }
}

/// State machine for decoding chunked `Transfer-Encoding`.
#[derive(Debug, Clone)]
pub struct ChunkedDecoder {
    state: ChunkedState,
    chunk_size: u64,
    saw_size_digit: bool,
    remaining: u64,
    trailer_line_empty: bool,
    metadata: MetadataBudget,
    ext_phase: ExtPhase,
    trailer_phase: TrailerPhase,
    /// `tchar`s consumed so far in the current trailer line's field-name,
    /// so a colon with none preceding it (`:value`) can be rejected as an
    /// empty name and a line's phase can decide, at its terminating CRLF,
    /// whether a colon was ever reached.
    trailer_name_len: usize,
}

/// Position within `chunk-ext = *( BWS ";" BWS chunk-ext-name [ BWS "="
/// BWS chunk-ext-val ] )`, `chunk-ext-val = token / quoted-string`
/// (RFC 9112 §7.1.1).
///
/// A byte allowlist alone cannot tell `;=x` (a value with no name, which
/// the grammar forbids) from `;a=x` (an ordinary extension): both consist
/// entirely of bytes the allowlist accepts. The grammar requires
/// structure the allowlist has no state to check, so this tracks where in
/// that structure the current byte falls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtPhase {
    /// Just consumed `;` (or the start of the first extension); a
    /// `chunk-ext-name` of at least one `tchar` must follow, after
    /// optional BWS.
    BeforeName,
    /// Reading `chunk-ext-name` (`token`): at least one `tchar` consumed.
    Name,
    /// BWS after the name, deciding between `=`, `;`, or the end.
    AfterName,
    /// BWS after `=`; a `chunk-ext-val` must follow.
    BeforeValue,
    /// Reading an unquoted `chunk-ext-val` (`token`).
    ValueToken,
    /// Inside a `quoted-string` value, after its opening `DQUOTE`.
    ValueQuoted,
    /// Just consumed the `\` of a `quoted-pair`; the escaped byte follows
    /// unconditionally.
    ValueQuotedEscaped,
    /// BWS after a value, deciding between `;` or the end.
    AfterValue,
}

/// Position within one `field-line = field-name ":" OWS field-value OWS`
/// of a trailer section (RFC 9112 §7.1.2, §5).
///
/// Distinguishes a field-name byte from a field-value byte, which a flat
/// allowlist cannot: the allowlist accepts every byte in `not-a-header`
/// just as it accepts every byte in `Trailer: value`, because nothing
/// about the bytes alone says whether a colon was ever required to
/// appear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrailerPhase {
    /// Reading `field-name` (`token`), before the colon.
    FieldName,
    /// After the colon: OWS, `field-value`, OWS. Not validated further
    /// than the existing byte allowlist — field-value's own grammar is
    /// permissive enough that the allowlist is a reasonable
    /// approximation, and the structural gap R06 calls out is the
    /// missing field-name/colon check, not field-value's fine grammar.
    AfterColon,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkedState {
    ReadingSize,
    ReadingExtension,
    ReadingSizeLf,
    ReadingData,
    ReadingDataCr,
    ReadingDataLf,
    ReadingTrailer,
    ReadingTrailerLf,
    Done,
    /// A verdict already reached, kept so it can be reported after the data
    /// that preceded it in the same call.
    Failed(ParseError),
}

/// What the chunked decoder wants the caller to do next.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodeResult {
    /// `n` bytes of decoded body data are in the output buffer.
    Data(usize),
    /// Need more input data.
    NeedMore,
    /// The chunked stream is complete.
    Done,
    /// A parse error occurred.
    Error(ParseError),
}

impl Default for ChunkedDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedDecoder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: ChunkedState::ReadingSize,
            chunk_size: 0,
            saw_size_digit: false,
            remaining: 0,
            trailer_line_empty: true,
            metadata: MetadataBudget::new(),
            ext_phase: ExtPhase::BeforeName,
            trailer_phase: TrailerPhase::FieldName,
            trailer_name_len: 0,
        }
    }

    #[must_use]
    pub const fn is_done(&self) -> bool {
        matches!(self.state, ChunkedState::Done)
    }

    /// Decode chunked data from `input` into `output`.
    ///
    /// Returns `(DecodeResult, bytes_consumed_from_input)`.
    pub fn decode(&mut self, input: &[u8], output: &mut [u8]) -> (DecodeResult, usize) {
        let mut in_pos = 0;
        let mut out_pos = 0;

        if let ChunkedState::Failed(e) = self.state {
            return (DecodeResult::Error(e), 0);
        }

        while in_pos < input.len() {
            match self.state {
                ChunkedState::ReadingSize
                | ChunkedState::ReadingExtension
                | ChunkedState::ReadingSizeLf => match self.read_size_line(&input[in_pos..]) {
                    Step::Advance(n) => in_pos += n,
                    Step::Yield((result, consumed)) => {
                        return self.deliver(result, in_pos + consumed, out_pos);
                    }
                    Step::EmitAndContinue(n) => {
                        in_pos += n;
                        if out_pos > 0 {
                            return (DecodeResult::Data(out_pos), in_pos);
                        }
                    }
                },
                ChunkedState::ReadingData => {
                    match self.read_data(&input[in_pos..], &mut output[out_pos..]) {
                        DataStep::Copied(n) => {
                            in_pos += n;
                            out_pos += n;
                        }
                        DataStep::BufferFull => {
                            return (DecodeResult::Data(out_pos), in_pos);
                        }
                    }
                }
                ChunkedState::ReadingDataCr | ChunkedState::ReadingDataLf => {
                    match self.read_data_terminator(&input[in_pos..]) {
                        Step::Advance(n) => in_pos += n,
                        Step::Yield((result, consumed)) => {
                            return self.deliver(result, in_pos + consumed, out_pos);
                        }
                        Step::EmitAndContinue(_) => unreachable!(),
                    }
                }
                ChunkedState::ReadingTrailer | ChunkedState::ReadingTrailerLf => {
                    match self.read_trailer(&input[in_pos..]) {
                        Step::Advance(n) => in_pos += n,
                        Step::Yield((result, consumed)) => {
                            return self.deliver(result, in_pos + consumed, out_pos);
                        }
                        Step::EmitAndContinue(_) => unreachable!(),
                    }
                }
                ChunkedState::Failed(e) => return (DecodeResult::Error(e), in_pos),
                ChunkedState::Done => break,
            }
        }

        if out_pos > 0 {
            (DecodeResult::Data(out_pos), in_pos)
        } else if self.is_done() {
            (DecodeResult::Done, in_pos)
        } else {
            (DecodeResult::NeedMore, in_pos)
        }
    }

    /// Deliver body bytes decoded before a verdict was reached, holding the
    /// verdict for the next call.
    ///
    /// Where a socket read happens to break is not something the peer chose,
    /// so it must not change what the caller receives. Returning an error
    /// while `output` holds decoded bytes discards them, which means a body
    /// split one way delivers data a body split another way loses. The same
    /// rule already governs `Done`, which `read_trailer` defers for exactly
    /// this reason.
    const fn deliver(
        &mut self,
        result: DecodeResult,
        consumed: usize,
        out_pos: usize,
    ) -> (DecodeResult, usize) {
        match result {
            DecodeResult::Error(e) if out_pos > 0 => {
                self.state = ChunkedState::Failed(e);
                (DecodeResult::Data(out_pos), consumed)
            }
            other => (other, consumed),
        }
    }

    fn read_size_line(&mut self, input: &[u8]) -> Step {
        for (i, &b) in input.iter().enumerate() {
            match self.state {
                ChunkedState::ReadingSize => {
                    if let Some(digit) = HexDigit::decode(b) {
                        if !self.metadata.charge_size_digit() {
                            return Step::Yield((
                                DecodeResult::Error(ParseError::InvalidChunkSize),
                                i,
                            ));
                        }
                        self.saw_size_digit = true;
                        self.chunk_size = match self
                            .chunk_size
                            .checked_mul(16)
                            .and_then(|v| v.checked_add(u64::from(digit)))
                        {
                            Some(v) => v,
                            None => {
                                return Step::Yield((
                                    DecodeResult::Error(ParseError::InvalidChunkSize),
                                    i,
                                ));
                            }
                        };
                    } else if (b == b'\r' || b == b';' || b == b' ' || b == b'\t')
                        && self.saw_size_digit
                    {
                        if b == b'\r' {
                            self.remaining = self.chunk_size;
                            self.state = ChunkedState::ReadingSizeLf;
                        } else {
                            // Entering `chunk-ext = *( BWS ";" BWS
                            // chunk-ext-name ... )` at its top: `b` is
                            // either the `;` opening the first extension or
                            // BWS still ahead of it, so the valid
                            // continuations here are exactly `AfterValue`'s
                            // (BWS, `;`, or the terminating CR) — there is
                            // no name yet to require, which is what sets
                            // this apart from `BeforeName`.
                            self.state = ChunkedState::ReadingExtension;
                            self.ext_phase = ExtPhase::AfterValue;
                            if let Some(err) = self.advance_extension(b) {
                                return Step::Yield((err, i));
                            }
                        }
                    } else {
                        return Step::Yield((DecodeResult::Error(ParseError::InvalidChunkSize), i));
                    }
                }
                ChunkedState::ReadingExtension => {
                    if let Some(err) = self.advance_extension(b) {
                        return Step::Yield((err, i));
                    }
                }
                ChunkedState::ReadingSizeLf => {
                    if b != b'\n' {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            i,
                        ));
                    }
                    if self.chunk_size == 0 {
                        self.state = ChunkedState::ReadingTrailer;
                        self.trailer_line_empty = true;
                        self.metadata.reset_extension();
                        // Hand control back to `decode`, which routes the
                        // remaining bytes to `read_trailer`. Falling through
                        // would re-enter this loop in `ReadingTrailer`, a
                        // state this function does not handle.
                        return Step::Advance(i + 1);
                    }
                    self.state = ChunkedState::ReadingData;
                    return Step::EmitAndContinue(i + 1);
                }
                _ => unreachable!(),
            }
        }
        Step::Advance(input.len())
    }

    fn read_data(&mut self, input: &[u8], output: &mut [u8]) -> DataStep {
        if output.is_empty() {
            return DataStep::BufferFull;
        }
        let to_copy = input
            .len()
            .min(output.len())
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        if to_copy == 0 {
            // Remaining is zero; next state should have been ReadingDataCr.
            self.state = ChunkedState::ReadingDataCr;
            return DataStep::Copied(0);
        }
        output[..to_copy].copy_from_slice(&input[..to_copy]);
        self.remaining -= u64::try_from(to_copy).unwrap_or(u64::MAX);
        if self.remaining == 0 {
            self.state = ChunkedState::ReadingDataCr;
        }
        DataStep::Copied(to_copy)
    }

    fn read_data_terminator(&mut self, input: &[u8]) -> Step {
        for (i, &b) in input.iter().enumerate() {
            match self.state {
                ChunkedState::ReadingDataCr => {
                    if b != b'\r' {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            i,
                        ));
                    }
                    self.state = ChunkedState::ReadingDataLf;
                }
                ChunkedState::ReadingDataLf => {
                    if b != b'\n' {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            i,
                        ));
                    }
                    self.chunk_size = 0;
                    self.saw_size_digit = false;
                    self.metadata.reset_extension();
                    self.metadata.reset_size_digits();
                    self.ext_phase = ExtPhase::BeforeName;
                    self.state = ChunkedState::ReadingSize;
                    return Step::Advance(i + 1);
                }
                _ => unreachable!(),
            }
        }
        Step::Advance(input.len())
    }

    fn read_trailer(&mut self, input: &[u8]) -> Step {
        for (i, &b) in input.iter().enumerate() {
            match self.state {
                ChunkedState::ReadingTrailer => {
                    if b == b'\r'
                        && self.trailer_phase == TrailerPhase::FieldName
                        && self.trailer_name_len == 0
                    {
                        self.state = ChunkedState::ReadingTrailerLf;
                        continue;
                    }
                    if !self.metadata.charge_trailer() {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkMetadata),
                            i,
                        ));
                    }
                    self.trailer_line_empty = false;
                    match self.trailer_phase {
                        TrailerPhase::FieldName if b == b':' && self.trailer_name_len > 0 => {
                            self.trailer_phase = TrailerPhase::AfterColon;
                        }
                        TrailerPhase::FieldName if Tchar::is_valid(b) => {
                            self.trailer_name_len += 1;
                        }
                        TrailerPhase::AfterColon if Self::is_metadata_byte(b) => {}
                        TrailerPhase::AfterColon if b == b'\r' => {
                            self.state = ChunkedState::ReadingTrailerLf;
                        }
                        _ => {
                            return Step::Yield((
                                DecodeResult::Error(ParseError::InvalidChunkMetadata),
                                i,
                            ));
                        }
                    }
                }
                ChunkedState::ReadingTrailerLf => {
                    if b != b'\n' {
                        return Step::Yield((
                            DecodeResult::Error(ParseError::InvalidChunkTerminator),
                            i,
                        ));
                    }
                    if self.trailer_line_empty {
                        self.state = ChunkedState::Done;
                        // Return control to `decode` instead of yielding
                        // `Done` directly: data decoded earlier in the same
                        // call must take precedence (`Data` before `Done`),
                        // which `decode`'s final `out_pos > 0` check handles.
                        // Yielding here would discard that pending data.
                        return Step::Advance(i + 1);
                    }
                    self.state = ChunkedState::ReadingTrailer;
                    self.trailer_line_empty = true;
                    self.trailer_phase = TrailerPhase::FieldName;
                    self.trailer_name_len = 0;
                }
                _ => unreachable!(),
            }
        }
        Step::Advance(input.len())
    }

    /// Whether `b` may appear inside a chunk extension or trailer line.
    ///
    /// Bare LF, NUL, and the other C0 controls are excluded: accepting them
    /// lets a peer smuggle line structure past a downstream parser that
    /// treats LF alone as a line ending. HTAB is allowed, as in field values.
    const fn is_metadata_byte(b: u8) -> bool {
        b == b'\t' || (b >= 0x20 && b != 0x7f)
    }

    /// Charge and advance one byte of `chunk-ext`, transitioning
    /// [`ChunkedState::ReadingExtension`] back to the size-line's CRLF when
    /// the extension ends.
    ///
    /// The terminating CR is not itself metadata — it belongs to the
    /// framing every chunk pays, not to what a peer can send unboundedly —
    /// so it is charged only when [`Self::step_extension`] says the byte
    /// was consumed *as* extension content, not when it ends the line.
    fn advance_extension(&mut self, b: u8) -> Option<DecodeResult> {
        match self.step_extension(b) {
            ExtStep::Continue => {
                if self.metadata.charge_extension() {
                    None
                } else {
                    Some(DecodeResult::Error(ParseError::InvalidChunkMetadata))
                }
            }
            ExtStep::EndOfLine => {
                self.remaining = self.chunk_size;
                self.state = ChunkedState::ReadingSizeLf;
                None
            }
            ExtStep::Invalid => Some(DecodeResult::Error(ParseError::InvalidChunkMetadata)),
        }
    }

    /// Advance [`Self::ext_phase`] by one byte of `chunk-ext = *( BWS ";"
    /// BWS chunk-ext-name [ BWS "=" BWS chunk-ext-val ] )` (RFC 9112
    /// §7.1.1), given that a `;` has already been consumed to reach
    /// [`ChunkedState::ReadingExtension`].
    ///
    /// `BWS` ("bad whitespace", RFC 9110 §5.6.3) is SP/HTAB tolerated around
    /// `;` and `=` for compatibility with existing senders; it is not
    /// permitted anywhere else the plain grammar does not name it.
    fn step_extension(&mut self, b: u8) -> ExtStep {
        let is_bws = |b: u8| b == b' ' || b == b'\t';
        // `BeforeName`/`BeforeValue` accept BWS without changing phase
        // (there is more BWS to come); `Name`/`AfterName`/`ValueToken`/
        // `AfterValue` accept it *into* `AfterName`/`AfterValue`, marking
        // that a token has ended even though more BWS may follow. Grouped
        // by the byte they react to rather than by phase, since several
        // phases share both the byte and the resulting transition.
        match self.ext_phase {
            ExtPhase::BeforeName | ExtPhase::BeforeValue if is_bws(b) => ExtStep::Continue,
            ExtPhase::Name | ExtPhase::AfterName | ExtPhase::ValueToken | ExtPhase::AfterValue
                if is_bws(b) =>
            {
                self.ext_phase = match self.ext_phase {
                    ExtPhase::ValueToken | ExtPhase::AfterValue => ExtPhase::AfterValue,
                    _ => ExtPhase::AfterName,
                };
                ExtStep::Continue
            }
            ExtPhase::BeforeName if Tchar::is_valid(b) => {
                self.ext_phase = ExtPhase::Name;
                ExtStep::Continue
            }
            ExtPhase::Name if Tchar::is_valid(b) => ExtStep::Continue,
            ExtPhase::Name | ExtPhase::AfterName if b == b'=' => {
                self.ext_phase = ExtPhase::BeforeValue;
                ExtStep::Continue
            }
            ExtPhase::Name | ExtPhase::AfterName | ExtPhase::ValueToken | ExtPhase::AfterValue
                if b == b';' =>
            {
                self.ext_phase = ExtPhase::BeforeName;
                ExtStep::Continue
            }
            ExtPhase::Name | ExtPhase::AfterName | ExtPhase::ValueToken | ExtPhase::AfterValue
                if b == b'\r' =>
            {
                ExtStep::EndOfLine
            }
            ExtPhase::BeforeValue if b == b'"' => {
                self.ext_phase = ExtPhase::ValueQuoted;
                ExtStep::Continue
            }
            ExtPhase::BeforeValue | ExtPhase::ValueToken if Tchar::is_valid(b) => {
                self.ext_phase = ExtPhase::ValueToken;
                ExtStep::Continue
            }
            // quoted-string = DQUOTE *( qdtext / quoted-pair ) DQUOTE
            // (RFC 9110 §5.6.4). qdtext excludes DQUOTE and "\"; a bare CR
            // or LF inside the quotes is likewise excluded by
            // `is_metadata_byte`, so a peer cannot use a quoted value to
            // smuggle a line ending past this state machine.
            ExtPhase::ValueQuoted if b == b'\\' => {
                self.ext_phase = ExtPhase::ValueQuotedEscaped;
                ExtStep::Continue
            }
            ExtPhase::ValueQuoted if b == b'"' => {
                self.ext_phase = ExtPhase::AfterValue;
                ExtStep::Continue
            }
            ExtPhase::ValueQuoted if Self::is_metadata_byte(b) => ExtStep::Continue,
            ExtPhase::ValueQuotedEscaped if Self::is_metadata_byte(b) => {
                self.ext_phase = ExtPhase::ValueQuoted;
                ExtStep::Continue
            }
            _ => ExtStep::Invalid,
        }
    }
}

/// What one byte of [`ChunkedDecoder::step_extension`] decided.
enum ExtStep {
    /// Still inside the extension; not yet the terminating CR.
    Continue,
    /// The byte was the CR that ends `chunk-ext`, consistent with the
    /// grammar reached so far.
    EndOfLine,
    /// The byte does not fit the grammar at the current phase.
    Invalid,
}

enum Step {
    Advance(usize),
    Yield((DecodeResult, usize)),
    EmitAndContinue(usize),
}

enum DataStep {
    Copied(usize),
    BufferFull,
}

/// Single hex-digit decoder used by the chunked decoder.
struct HexDigit;

impl HexDigit {
    const fn decode(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked_simple() {
        let input = b"5\r\nhello\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, consumed) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(5));
        assert_eq!(&output[..5], b"hello");

        let (result, _) = decoder.decode(&input[consumed..], &mut output);
        assert_eq!(result, DecodeResult::Done);
        assert!(decoder.is_done());
    }

    #[test]
    fn chunked_multiple_chunks() {
        let input = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done | DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"hello world");
        assert!(decoder.is_done());
    }

    #[test]
    fn data_before_an_error_is_delivered_not_discarded() {
        // Found by fuzzing: fed whole, this lost the "a" that a byte-at-a-time
        // feed of the same bytes delivered. Where a socket read happens to
        // break is not something the peer chose, so it must not change what
        // the caller receives.
        let input = b"1\r\na\r\nz\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];

        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(1));
        assert_eq!(&output[..1], b"a");

        let (result, consumed) = decoder.decode(b"anything", &mut output);
        assert_eq!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize),
            "the held verdict must arrive on the next call"
        );
        assert_eq!(consumed, 0, "a failed decoder consumes no further input");
    }

    #[test]
    fn a_failed_decoder_reports_the_same_error_forever() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        decoder.decode(b"1\r\na\r\nz\r\n", &mut output);

        for _ in 0..3 {
            let (result, _) = decoder.decode(b"0\r\n\r\n", &mut output);
            assert_eq!(result, DecodeResult::Error(ParseError::InvalidChunkSize));
        }
        assert!(!decoder.is_done(), "a stream that failed never completed");
    }

    #[test]
    fn an_error_with_no_pending_data_is_reported_immediately() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(b"z\r\n", &mut output);
        assert_eq!(result, DecodeResult::Error(ParseError::InvalidChunkSize));
    }

    #[test]
    fn chunked_byte_at_a_time() {
        let input = b"3\r\nabc\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();

        for &byte in input {
            let one = [byte];
            let (result, _) = decoder.decode(&one, &mut output);
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"abc");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunked_with_extension() {
        let input = b"5;ext=val\r\nhello\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, consumed) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(5));
        assert_eq!(&output[..5], b"hello");

        let (result, _) = decoder.decode(&input[consumed..], &mut output);
        assert_eq!(result, DecodeResult::Done);
    }

    #[test]
    fn chunked_with_trailers() {
        let input = b"5\r\nhello\r\n0\r\nTrailer: value\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done | DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"hello");
        assert!(decoder.is_done());
    }

    /// Feed `input` until the decoder yields a terminal result.
    fn drive(decoder: &mut ChunkedDecoder, input: &[u8]) -> DecodeResult {
        let mut output = [0u8; 256];
        let mut pos = 0;
        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(_) if pos < input.len() => {}
                other => return other,
            }
        }
    }

    #[test]
    fn oversized_chunk_extension_is_rejected() {
        // Extensions carry no payload, so an unbounded one lets a peer occupy
        // the connection forever without delivering a single body byte.
        let mut input = b"5;".to_vec();
        input.extend(std::iter::repeat_n(b'x', MAX_CHUNK_EXTENSION + 1));
        input.extend_from_slice(b"\r\nhello\r\n0\r\n\r\n");

        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, &input),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn extension_within_the_budget_is_accepted() {
        // The bound must not reject ordinary extensions.
        let mut input = b"5;".to_vec();
        input.extend(std::iter::repeat_n(b'x', MAX_CHUNK_EXTENSION - 1));
        input.extend_from_slice(b"\r\nhello\r\n0\r\n\r\n");

        let mut decoder = ChunkedDecoder::new();
        assert_eq!(drive(&mut decoder, &input), DecodeResult::Data(5));
    }

    #[test]
    fn extension_budget_resets_between_chunks() {
        // Each chunk gets its own extension allowance; a long stream of
        // normally-extended chunks must not accumulate into a refusal.
        let mut input = Vec::new();
        for _ in 0..8 {
            input.extend_from_slice(b"1;");
            input.extend(std::iter::repeat_n(b'x', MAX_CHUNK_EXTENSION - 1));
            input.extend_from_slice(b"\r\na\r\n");
        }
        input.extend_from_slice(b"0\r\n\r\n");

        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 256];
        let mut total = Vec::new();
        let mut pos = 0;
        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                other => panic!("unexpected result: {other:?}"),
            }
        }
        assert_eq!(total, b"aaaaaaaa");
    }

    #[test]
    fn oversized_trailer_section_is_rejected() {
        // Many small trailer lines share one budget, so the section cannot be
        // extended indefinitely by splitting it up. CRLF is not charged, so
        // the loop counts the field bytes the budget actually sees.
        const LINE: &[u8] = b"X-Pad: value\r\n";
        let charged_per_line = LINE.len() - 2;
        let lines = MAX_TRAILER_SECTION / charged_per_line + 2;

        let mut input = b"0\r\n".to_vec();
        for _ in 0..lines {
            input.extend_from_slice(LINE);
        }
        input.extend_from_slice(b"\r\n");

        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, &input),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn bare_lf_in_a_chunk_extension_is_rejected() {
        // A bare LF inside metadata lets a peer smuggle line structure past a
        // downstream parser that treats LF alone as a line ending.
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;ext\nvalue\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn nul_in_a_chunk_extension_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;ext\0bad\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn bare_lf_in_a_trailer_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"0\r\nTrailer: a\nb\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    // ── R06: chunk-ext / trailer grammar, not just a byte allowlist ──────────

    /// `1;=\r\na\r\n0\r\n\r\n`: an extension value with no name. The byte
    /// allowlist alone accepts every byte here; only the grammar's
    /// requirement of a `chunk-ext-name` before `=` catches it.
    #[test]
    fn a_chunk_extension_value_without_a_name_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"1;=\r\na\r\n0\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    /// `0\r\nnot-a-header\r\n\r\n`: a trailer line with no colon. The byte
    /// allowlist accepts `not-a-header` outright; only requiring a colon
    /// before the field-value grammar catches it.
    #[test]
    fn a_trailer_line_without_a_colon_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"0\r\nnot-a-header\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    /// A colon with nothing before it is an empty field-name, which
    /// `field-name = token` (at least one `tchar`) forbids just as it
    /// forbids a missing colon.
    #[test]
    fn a_trailer_line_with_an_empty_field_name_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"0\r\n: value\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    /// A bare `;` opening an extension with nothing after it is not a
    /// `chunk-ext-name` (which needs at least one `tchar`) followed by the
    /// terminating CRLF — the extension is unterminated when the CR arrives
    /// straight after `;`.
    #[test]
    fn a_bare_semicolon_with_no_extension_name_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"1;\r\na\r\n0\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    /// The ordinary form the grammar exists to accept: a named extension
    /// with an unquoted token value.
    #[test]
    fn a_well_formed_chunk_extension_with_a_token_value_is_accepted() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;ext=value\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Data(5)
        );
    }

    /// `chunk-ext-name` alone, with no `= chunk-ext-val`, is valid: the
    /// value is optional.
    #[test]
    fn a_chunk_extension_with_no_value_is_accepted() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;ext\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Data(5)
        );
    }

    /// Multiple extensions on one chunk, name-only and name=value mixed.
    #[test]
    fn multiple_chunk_extensions_on_one_chunk_are_accepted() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;a;b=c;d=\"e\"\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Data(5)
        );
    }

    /// A quoted value may contain a `;` and a `=` without those bytes being
    /// mistaken for extension structure — they are `qdtext` inside the
    /// quotes, not delimiters.
    #[test]
    fn a_quoted_extension_value_may_contain_delimiter_bytes() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;ext=\"a;b=c\"\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Data(5)
        );
    }

    /// `quoted-pair` lets a quoted value escape its own closing quote; the
    /// escaped `"` must not end the string early.
    #[test]
    fn a_quoted_extension_value_may_escape_a_quote() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;ext=\"a\\\"b\"\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Data(5)
        );
    }

    /// An unterminated quoted value — a CR arrives before the closing quote
    /// — is invalid rather than treated as ending the extension.
    #[test]
    fn an_unterminated_quoted_extension_value_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"5;ext=\"unterminated\r\nhello\r\n0\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    /// RFC 9112 §7.1.1 reintroduces BWS (bad whitespace) around `;` and `=`
    /// for compatibility with existing senders.
    #[test]
    fn whitespace_around_extension_delimiters_is_tolerated() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(
                &mut decoder,
                b"5 ; ext = value ; other\r\nhello\r\n0\r\n\r\n"
            ),
            DecodeResult::Data(5)
        );
    }

    /// The ordinary trailer form: field-name, colon, OWS, value.
    #[test]
    fn a_well_formed_trailer_field_is_accepted() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"0\r\nX-Checksum: abc123\r\n\r\n"),
            DecodeResult::Done
        );
    }

    /// A trailer value may itself be empty (`field-value` permits zero
    /// octets), so a bare `Name:` line must not be rejected as if it had no
    /// colon at all.
    #[test]
    fn a_trailer_field_with_an_empty_value_is_accepted() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"0\r\nX-Empty:\r\n\r\n"),
            DecodeResult::Done
        );
    }

    /// A field-name byte the token grammar forbids (here, a space) must be
    /// rejected even though it would pass the old byte allowlist.
    #[test]
    fn a_trailer_field_name_with_a_non_token_byte_is_rejected() {
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(
            drive(&mut decoder, b"0\r\nBad Name: value\r\n\r\n"),
            DecodeResult::Error(ParseError::InvalidChunkMetadata)
        );
    }

    #[test]
    fn chunked_hex_cases() {
        let input = b"A\r\n0123456789\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(10));
        assert_eq!(&output[..10], b"0123456789");

        let input = b"a\r\n0123456789\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Data(10));
    }

    #[test]
    fn chunked_small_output_buffer() {
        let input = b"a\r\n0123456789\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 4];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done | DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"0123456789");
        assert!(decoder.is_done());
    }

    // ── Adversarial chunked decoder ──────────────────────────────────────

    #[test]
    fn chunk_size_overflow() {
        let input = b"FFFFFFFFFFFFFFFF0\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(input, &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    #[test]
    fn chunk_size_many_leading_zeros_decodes_normally() {
        for zeros in [255usize, 256, 257, 512] {
            let mut input = vec![b'0'; zeros];
            input.extend_from_slice(b"7\r\nabcdefg\r\n0\r\n\r\n");

            let mut decoder = ChunkedDecoder::new();
            let mut output = [0u8; 64];
            let mut total = Vec::new();
            let mut pos = 0;

            loop {
                let (result, consumed) = decoder.decode(&input[pos..], &mut output);
                pos += consumed;
                match result {
                    DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                    DecodeResult::Done => break,
                    DecodeResult::NeedMore => {}
                    DecodeResult::Error(e) => panic!("unexpected error at {zeros} zeros: {e}"),
                }
            }

            assert_eq!(total, b"abcdefg", "{zeros} leading zeros");
            assert!(decoder.is_done(), "{zeros} leading zeros");
        }
    }

    #[test]
    fn chunk_size_leading_zeros_split_across_feeds() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();

        for _ in 0..300 {
            let (result, _) = decoder.decode(b"0", &mut output);
            assert_eq!(result, DecodeResult::NeedMore);
        }

        let rest = b"7\r\nabcdefg\r\n0\r\n\r\n";
        let mut pos = 0;
        loop {
            let (result, consumed) = decoder.decode(&rest[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"abcdefg");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunk_size_leading_zeros() {
        let input = b"007\r\nabcdefg\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"abcdefg");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunk_missing_crlf_after_data() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];

        // Feed size + data in one call so decoder yields the body data.
        let (result, _) = decoder.decode(b"5\r\nhello", &mut output);
        assert_eq!(result, DecodeResult::Data(5));
        assert_eq!(&output[..5], b"hello");

        // Now feed 'X' where \r\n was expected.
        let (result, _) = decoder.decode(b"X", &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkTerminator)
        ));
    }

    #[test]
    fn chunk_decode_empty_input() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, consumed) = decoder.decode(b"", &mut output);
        assert_eq!(result, DecodeResult::NeedMore);
        assert_eq!(consumed, 0);
    }

    #[test]
    fn chunk_multiple_trailers() {
        let input = b"5\r\nhello\r\n0\r\nTrailer-A: 1\r\nTrailer-B: 2\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"hello");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunk_extension_with_quoted_value() {
        let input = b"5;ext=\"val\"\r\nhello\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();
        let mut pos = 0;

        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"hello");
    }

    #[test]
    fn chunk_interleaved_partial_feeds() {
        // Feed the chunk size partially, then the rest
        let part1 = b"1";
        let part2 = b"0\r\n0123456789abcdef\r\n0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let mut total = Vec::new();

        let (result, _) = decoder.decode(part1, &mut output);
        assert_eq!(result, DecodeResult::NeedMore);

        let mut pos = 0;
        loop {
            let (result, consumed) = decoder.decode(&part2[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                DecodeResult::NeedMore => {}
                DecodeResult::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(total, b"0123456789abcdef");
        assert!(decoder.is_done());
    }

    #[test]
    fn chunk_empty_size_line_rejected() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(b"\r\n", &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    #[test]
    fn chunk_extension_without_size_rejected() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(b";ext\r\n", &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    #[test]
    fn chunk_empty_size_line_after_data_rejected() {
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let wire = b"5\r\nhello\r\n";
        let (result, consumed) = decoder.decode(wire, &mut output);
        assert_eq!(result, DecodeResult::Data(5));
        assert_eq!(consumed, wire.len());
        let (result, _) = decoder.decode(b"\r\n\r\n", &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    /// Leading-zero digits are not charged for free forever: past
    /// `MAX_CHUNK_SIZE_DIGITS` they can only be padding, and the decoder
    /// must refuse rather than keep consuming input without ever reaching
    /// a chunk boundary.
    #[test]
    fn chunk_size_digits_past_the_bound_are_rejected() {
        let input = vec![b'0'; MAX_CHUNK_SIZE_DIGITS + 1];
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(&input, &mut output);
        assert_eq!(result, DecodeResult::Error(ParseError::InvalidChunkSize));
    }

    /// The converse: a size line at exactly the bound must still decode, so
    /// the limit is not off by one against ordinary large-but-finite input.
    #[test]
    fn chunk_size_digits_at_the_bound_are_accepted() {
        let mut input = vec![b'0'; MAX_CHUNK_SIZE_DIGITS - 1];
        input.extend_from_slice(b"7\r\nabcdefg\r\n0\r\n\r\n");
        let mut decoder = ChunkedDecoder::new();
        assert_eq!(drive(&mut decoder, &input), DecodeResult::Data(7));
    }

    /// The digit budget is per chunk, like the extension budget: a long
    /// stream of ordinarily-padded chunk sizes must not accumulate into a
    /// spurious refusal.
    #[test]
    fn chunk_size_digit_budget_resets_between_chunks() {
        let mut input = Vec::new();
        for _ in 0..8 {
            input.extend(std::iter::repeat_n(b'0', MAX_CHUNK_SIZE_DIGITS - 1));
            input.extend_from_slice(b"1\r\na\r\n");
        }
        input.extend_from_slice(b"0\r\n\r\n");

        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 256];
        let mut total = Vec::new();
        let mut pos = 0;
        loop {
            let (result, consumed) = decoder.decode(&input[pos..], &mut output);
            pos += consumed;
            match result {
                DecodeResult::Data(n) => total.extend_from_slice(&output[..n]),
                DecodeResult::Done => break,
                other => panic!("unexpected result: {other:?}"),
            }
        }
        assert_eq!(total, b"aaaaaaaa");
    }

    #[test]
    fn chunk_invalid_hex_in_size() {
        let input = b"ZZ\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(input, &mut output);
        assert!(matches!(
            result,
            DecodeResult::Error(ParseError::InvalidChunkSize)
        ));
    }

    #[test]
    fn chunk_empty_body_immediate_terminator() {
        let input = b"0\r\n\r\n";
        let mut decoder = ChunkedDecoder::new();
        let mut output = [0u8; 64];
        let (result, _) = decoder.decode(input, &mut output);
        assert_eq!(result, DecodeResult::Done);
        assert!(decoder.is_done());
    }
}
