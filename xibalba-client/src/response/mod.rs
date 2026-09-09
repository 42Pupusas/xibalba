//! Response head I/O and the public response types built from it.
//!
//! `head` owns `HeadData` and the socket-level work of reading a head off
//! the wire (skipping 1xx interims, deriving body framing); `public` holds
//! [`Response`] and [`StreamingResponse`], the types a caller actually gets
//! back. Kept apart because they answer different questions: one is about
//! how bytes become a head, the other about what a finished or in-flight
//! response looks like to the caller.

mod head;
mod public;

pub(crate) use head::HeadData;
pub use public::{Response, StreamingResponse};
