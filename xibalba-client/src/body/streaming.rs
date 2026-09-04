use std::io::Read;

use xibalba_proto::error::Error;

use crate::config::HEAD_BUF_SIZE;
use crate::silence::SilenceBudget;

/// Incremental body reader over a live connection stream.
///
/// Unlike the fully-buffered [`BodyCollector`](crate::body::BodyCollector),
/// this decodes framing (chunked / content-length / until-close) on the fly
/// as the caller reads — required for server-sent events, where the
/// response only ends after the server is done generating.
///
/// Holds `&mut` borrows of the client's stream and its dirty flag for
/// the duration of the response. The flag is cleared when the body is
/// read to completion; if the reader is dropped early, the flag stays
/// set and the client reconnects before its next request instead of
/// reading stale bytes.
#[derive(Debug)]
pub struct StreamingBody<'a, S: Read> {
    stream: &'a mut S,
    dirty: &'a mut bool,
    state: StreamState,
    /// Raw bytes already pulled off the socket but not yet decoded:
    /// first the head-read overshoot, then refills from `stream`.
    raw: Vec<u8>,
    raw_pos: usize,
    /// Wall-clock silence tolerance between body bytes; resets on
    /// every successful socket read. See [`SilenceBudget`].
    budget: SilenceBudget,
}

#[derive(Debug)]
enum StreamState {
    Length {
        remaining: u64,
    },
    Chunked {
        decoder: xibalba_proto::response::ChunkedDecoder,
    },
    UntilClose,
    Done,
}

enum ChunkedRead {
    Progress(usize),
    NeedMore,
}

impl<'a, S: Read> StreamingBody<'a, S> {
    pub(crate) fn new(
        stream: &'a mut S,
        dirty: &'a mut bool,
        framing: &xibalba_proto::response::BodyFraming,
        tail: Vec<u8>,
        silence: std::time::Duration,
    ) -> Self {
        use xibalba_proto::response::{BodyFraming, ChunkedDecoder};
        let state = match *framing {
            BodyFraming::ContentLength(0) | BodyFraming::None => StreamState::Done,
            BodyFraming::ContentLength(len) => StreamState::Length { remaining: len },
            BodyFraming::Chunked => StreamState::Chunked {
                decoder: ChunkedDecoder::new(),
            },
            BodyFraming::UntilClose => StreamState::UntilClose,
        };
        let mut body = Self {
            stream,
            dirty,
            state,
            raw: tail,
            raw_pos: 0,
            budget: SilenceBudget::new(silence),
        };
        if matches!(body.state, StreamState::Done) {
            body.finish();
        }
        body
    }

    /// Whether the body has been fully consumed.
    #[must_use]
    pub const fn is_done(&self) -> bool {
        matches!(self.state, StreamState::Done)
    }

    const fn finish(&mut self) {
        self.state = StreamState::Done;
        *self.dirty = self.raw_pos != self.raw.len();
    }

    /// Bytes available without touching the socket; refills from the
    /// socket when empty. `Ok(&[])` means clean EOF from the peer.
    fn input(&mut self) -> std::io::Result<&[u8]> {
        self.refill()?;
        Ok(&self.raw[self.raw_pos..])
    }

    /// Refill the raw buffer; `Ok(false)` means the peer closed.
    fn refill(&mut self) -> std::io::Result<bool> {
        if self.raw_pos < self.raw.len() {
            return Ok(true);
        }
        self.raw.resize(HEAD_BUF_SIZE, 0);
        let n = self.budget.read(&mut self.stream, &mut self.raw)?;
        self.raw.truncate(n);
        self.raw_pos = 0;
        Ok(n > 0)
    }

    fn read_content_length(&mut self, buf: &mut [u8], remaining: u64) -> std::io::Result<usize> {
        let input = self.input()?;
        if input.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed mid-body",
            ));
        }
        let n = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(input.len())
            .min(buf.len());
        buf[..n].copy_from_slice(&input[..n]);
        self.raw_pos += n;
        let remaining = remaining - u64::try_from(n).unwrap_or(u64::MAX);
        if remaining == 0 {
            self.finish();
        } else {
            self.state = StreamState::Length { remaining };
        }
        Ok(n)
    }

    fn read_until_close(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let input = self.input()?;
        if input.is_empty() {
            self.state = StreamState::Done;
            // Connection is dead; leave the dirty flag set
            // so the client reconnects.
            return Ok(0);
        }
        let n = input.len().min(buf.len());
        buf[..n].copy_from_slice(&input[..n]);
        self.raw_pos += n;
        Ok(n)
    }

    fn read_chunked(&mut self, buf: &mut [u8]) -> std::io::Result<ChunkedRead> {
        use xibalba_proto::response::DecodeResult;

        if !self.refill()? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed mid-chunked-body",
            ));
        }

        let StreamState::Chunked { decoder } = &mut self.state else {
            unreachable!("read_chunked called outside Chunked state");
        };

        let (result, consumed) = decoder.decode(&self.raw[self.raw_pos..], buf);
        self.raw_pos += consumed;
        match result {
            DecodeResult::Data(n) => {
                if decoder.is_done() {
                    self.finish();
                }
                Ok(ChunkedRead::Progress(n))
            }
            DecodeResult::Done => {
                self.finish();
                Ok(ChunkedRead::Progress(0))
            }
            DecodeResult::NeedMore => Ok(ChunkedRead::NeedMore),
            DecodeResult::Error(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                Error::Parse(e).to_string(),
            )),
        }
    }
}

impl<S: Read> Read for StreamingBody<'_, S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        match self.state {
            StreamState::Done => Ok(0),
            StreamState::Length { remaining } => self.read_content_length(buf, remaining),
            StreamState::UntilClose => self.read_until_close(buf),
            StreamState::Chunked { .. } => {
                // Loop until the decoder either emits data or finishes the
                // chunked stream. Returning `Ok(0)` here would signal EOF to
                // callers, which is wrong when the decoder is merely waiting
                // for the next chunk.
                loop {
                    match self.read_chunked(buf)? {
                        ChunkedRead::Progress(0) if self.is_done() => return Ok(0),
                        ChunkedRead::Progress(n) => return Ok(n),
                        ChunkedRead::NeedMore => {}
                    }
                }
            }
        }
    }
}
