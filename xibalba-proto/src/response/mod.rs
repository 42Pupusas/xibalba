//! Response head parsing, body framing, chunked-transfer decoding, and
//! header byte-offset ranges.
//!
//! Split by responsibility rather than kept as one file: `head` owns the
//! status-line-and-headers parse, `framing` decides how the body is
//! delimited, `chunked` is the `Transfer-Encoding: chunked` state machine,
//! and `ranges` computes byte offsets for a caller that wants positions
//! into the original buffer rather than borrowed slices. Every public item
//! is re-exported here so existing `xibalba_proto::response::*` paths are
//! unaffected by the split.

mod chunked;
mod framing;
mod head;
mod ranges;

pub use chunked::{
    ChunkedDecoder, DecodeResult, MAX_CHUNK_EXTENSION, MAX_CHUNK_SIZE_DIGITS, MAX_TRAILER_SECTION,
};
pub use framing::BodyFraming;
pub use head::ResponseHead;
pub use ranges::{HeaderRange, MAX_ADDRESSABLE_HEAD, MAX_HEADERS};
