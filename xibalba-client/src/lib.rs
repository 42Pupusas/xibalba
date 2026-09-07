//! A blocking and a background-reader HTTP/1.1 client over [`xibalba_proto`].
//!
//! - [`client::Client`] — one connection, caller-driven, on the calling
//!   thread. Start with [`Client::connect`] and [`Client::get`].
//! - [`async_client::AsyncClient`] — the same client on a dedicated reader
//!   thread, responses delivered as chunks over ring buffers.

pub mod admission;
pub mod async_client;
pub mod body;
pub mod client;
pub mod config;
pub mod connector;
pub mod control;
pub mod dial;
pub mod interrupt;
pub mod params;
pub mod redirect;
pub mod reference;
pub mod response;

mod delivery;
mod reader;
mod reuse;
mod silence;

pub use xibalba_proto as proto;

pub use admission::{Admission, DEFAULT_MAX_OUTSTANDING};
pub use async_client::{AsyncClient, Chunk, StreamHandle};
pub use client::Client;
pub use config::{Config, DEFAULT_MAX_HEAD_SIZE, HEAD_BUF_SIZE};
pub use control::AsyncRequest;
pub use delivery::ChunkStream;
pub use interrupt::{Interrupt, InterruptibleStream, NeverCancelled};
pub use params::RequestBuilder;
pub use response::{Response, StreamingResponse};
