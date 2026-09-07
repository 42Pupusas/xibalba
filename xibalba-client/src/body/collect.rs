use std::io::Read;
use std::time::Duration;

use xibalba_proto::error::{ConnectionError, Error};

use crate::config::HEAD_BUF_SIZE;
use crate::silence::{RequestDeadline, SilenceBudget};

/// Buffered-body reader: reads one complete response body into memory
/// according to its framing, enforcing the client's `max_response_body`.
///
/// Existed as the free function `read_body`; a struct gives the
/// framing-specific paths an owner and keeps the buffer-sizing choices
/// (`HEAD_BUF_SIZE` scratch, capacity preset) local to the type that makes
/// them.
pub(crate) struct BodyCollector {
    max_body: usize,
    budget: SilenceBudget,
    reusable: bool,
}

impl BodyCollector {
    /// A collector whose reads also answer to a total deadline for the
    /// operation that asked for the body. [`RequestDeadline::NONE`] gives
    /// plain gap-bounded collection.
    pub(crate) fn with_deadline(
        max_body: usize,
        silence: Duration,
        deadline: RequestDeadline,
    ) -> Self {
        Self {
            max_body,
            budget: SilenceBudget::with_deadline(silence, deadline),
            reusable: true,
        }
    }

    pub(crate) fn read<S: Read>(
        &mut self,
        stream: &mut S,
        framing: &xibalba_proto::response::BodyFraming,
        tail: &[u8],
    ) -> Result<Vec<u8>, Error> {
        use xibalba_proto::response::BodyFraming as ProtoFraming;

        self.reusable = true;
        match *framing {
            ProtoFraming::None => {
                self.reusable = tail.is_empty();
                Ok(Vec::new())
            }
            ProtoFraming::ContentLength(len) => self.read_content_length(stream, len, tail),
            ProtoFraming::Chunked => self.read_chunked(stream, tail),
            ProtoFraming::UntilClose => {
                self.reusable = false;
                self.read_until_close(stream, tail)
            }
        }
    }

    #[must_use]
    pub(crate) const fn is_reusable(&self) -> bool {
        self.reusable
    }

    fn read_content_length<S: Read>(
        &mut self,
        stream: &mut S,
        len: u64,
        tail: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let len = usize::try_from(len)
            .map_err(|_| Error::from(ConnectionError::ContentLengthOverflow))?;
        if len > self.max_body {
            return Err(ConnectionError::BodyTooLarge.into());
        }
        self.reusable = tail.len() <= len;
        let mut body = Vec::with_capacity(len);
        let from_tail = tail.len().min(len);
        body.extend_from_slice(&tail[..from_tail]);
        if body.len() < len {
            body.resize(len, 0);
            self.budget
                .read_exact_proto(stream, &mut body[from_tail..])?;
        }
        Ok(body)
    }

    fn read_chunked<S: Read>(&mut self, stream: &mut S, tail: &[u8]) -> Result<Vec<u8>, Error> {
        use xibalba_proto::response::{ChunkedDecoder, DecodeResult};

        let mut decoder = ChunkedDecoder::new();
        let mut body = Vec::new();
        let mut raw = [0u8; HEAD_BUF_SIZE];

        let mut input = tail;
        loop {
            let mut decode_buf = [0u8; HEAD_BUF_SIZE];
            let (result, consumed) = decoder.decode(input, &mut decode_buf);
            input = &input[consumed..];
            match result {
                DecodeResult::Data(n) => {
                    body.extend_from_slice(&decode_buf[..n]);
                    if body.len() > self.max_body {
                        return Err(ConnectionError::BodyTooLarge.into());
                    }
                    if decoder.is_done() {
                        self.reusable = input.is_empty();
                        return Ok(body);
                    }
                }
                DecodeResult::Done => {
                    self.reusable = input.is_empty();
                    return Ok(body);
                }
                DecodeResult::NeedMore => break,
                DecodeResult::Error(e) => return Err(Error::Parse(e)),
            }
        }

        loop {
            let n = self.budget.read_proto(stream, &mut raw)?;
            if n == 0 {
                return Err(ConnectionError::ConnectionClosed.into());
            }
            let mut pos = 0;
            while pos < n {
                let mut decode_buf = [0u8; HEAD_BUF_SIZE];
                let (result, consumed) = decoder.decode(&raw[pos..n], &mut decode_buf);
                pos += consumed;
                match result {
                    DecodeResult::Data(dn) => {
                        body.extend_from_slice(&decode_buf[..dn]);
                        if body.len() > self.max_body {
                            return Err(ConnectionError::BodyTooLarge.into());
                        }
                        if decoder.is_done() {
                            self.reusable = pos == n;
                            return Ok(body);
                        }
                    }
                    DecodeResult::Done => {
                        self.reusable = pos == n;
                        return Ok(body);
                    }
                    DecodeResult::NeedMore => break,
                    DecodeResult::Error(e) => return Err(Error::Parse(e)),
                }
            }
        }
    }

    fn read_until_close<S: Read>(&mut self, stream: &mut S, tail: &[u8]) -> Result<Vec<u8>, Error> {
        // The tail arrives with the head and can exceed the limit on its own,
        // so it is checked before the first read rather than after it.
        if tail.len() > self.max_body {
            return Err(ConnectionError::BodyTooLarge.into());
        }
        let mut body = tail.to_vec();
        let mut raw = [0u8; HEAD_BUF_SIZE];
        loop {
            let n = self.budget.read_proto(stream, &mut raw)?;
            if n == 0 {
                return Ok(body);
            }
            body.extend_from_slice(&raw[..n]);
            if body.len() > self.max_body {
                return Err(ConnectionError::BodyTooLarge.into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use xibalba_proto::response::BodyFraming;

    const BUDGET: Duration = Duration::from_secs(5);

    #[test]
    fn until_close_tail_alone_over_the_limit_is_rejected() {
        // A head and a small close-delimited body can arrive in one read, so
        // the tail is already over the limit before any further read. An
        // immediate EOF must not hand that body back unchecked.
        let mut collector = BodyCollector::with_deadline(4, BUDGET, RequestDeadline::NONE);
        let err = collector
            .read(
                &mut Cursor::new(b""),
                &BodyFraming::UntilClose,
                b"0123456789",
            )
            .expect_err("a tail larger than max_body must be rejected");
        assert!(matches!(
            err,
            Error::Connection(ConnectionError::BodyTooLarge)
        ));
    }

    #[test]
    fn until_close_overflow_is_rejected() {
        let mut collector = BodyCollector::with_deadline(4, BUDGET, RequestDeadline::NONE);
        let err = collector
            .read(
                &mut Cursor::new(b"0123456789"),
                &BodyFraming::UntilClose,
                b"",
            )
            .expect_err("body exceeding max_body must be rejected");
        assert!(matches!(
            err,
            Error::Connection(ConnectionError::BodyTooLarge)
        ));
    }

    #[test]
    fn until_close_within_limit_is_accepted() {
        let mut collector = BodyCollector::with_deadline(10, BUDGET, RequestDeadline::NONE);
        let body = collector
            .read(
                &mut Cursor::new(b"0123456789"),
                &BodyFraming::UntilClose,
                b"",
            )
            .unwrap();
        assert_eq!(body, b"0123456789");
    }

    #[test]
    fn until_close_tail_prefixes_the_body() {
        let mut collector = BodyCollector::with_deadline(10, BUDGET, RequestDeadline::NONE);
        let body = collector
            .read(&mut Cursor::new(b"567"), &BodyFraming::UntilClose, b"01234")
            .unwrap();
        assert_eq!(body, b"01234567");
    }

    #[test]
    fn content_length_at_the_limit_is_accepted() {
        let mut collector = BodyCollector::with_deadline(4, BUDGET, RequestDeadline::NONE);
        let body = collector
            .read(
                &mut Cursor::new(b"34"),
                &BodyFraming::ContentLength(4),
                b"01",
            )
            .unwrap();
        assert_eq!(body, b"0134");
    }

    #[test]
    fn chunked_overflow_is_rejected() {
        let mut collector = BodyCollector::with_deadline(4, BUDGET, RequestDeadline::NONE);
        let wire = b"4\r\n0123\r\n4\r\n4567\r\n0\r\n\r\n";
        let err = collector
            .read(&mut Cursor::new(&wire[..]), &BodyFraming::Chunked, b"")
            .expect_err("body exceeding max_body must be rejected");
        assert!(matches!(
            err,
            Error::Connection(ConnectionError::BodyTooLarge)
        ));
    }
}
