//! Response-body readers: fully buffered, incrementally decoded, and the
//! silence budget they share.

mod collect;
mod streaming;

pub(crate) use collect::BodyCollector;
pub use streaming::StreamingBody;

use std::io::Read;

/// In-memory body reader with a specialised `read_to_end` that does a single
/// `extend_from_slice` instead of reading through the `Read` trait.
#[derive(Debug)]
pub struct BodyReader {
    data: Vec<u8>,
    pos: usize,
}

impl BodyReader {
    pub(crate) const fn new(data: Vec<u8>) -> Self {
        Self { data, pos: 0 }
    }
}

impl Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.data[self.pos..];
        let n = remaining.len().min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.pos += n;
        Ok(n)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        let remaining = &self.data[self.pos..];
        let n = remaining.len();
        buf.extend_from_slice(remaining);
        self.pos = self.data.len();
        Ok(n)
    }
}
