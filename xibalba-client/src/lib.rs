//! A blocking and a background-reader HTTP/1.1 client over [`xibalba_proto`].
//!
//! - [`client::Client`] — one connection, caller-driven, on the calling
//!   thread. Start with [`Client::connect`] and [`Client::get`].
//! - [`async_client::AsyncClient`] — the same client on a dedicated reader
//!   thread, responses delivered as chunks over ring buffers.

pub mod async_client;
pub mod body;
pub mod client;
pub mod config;
pub mod connector;
pub mod params;
pub mod redirect;
pub mod response;

mod delivery;
mod reuse;
mod silence;

pub use xibalba_proto as proto;

pub use async_client::{AsyncClient, AsyncRequest, Chunk, StreamHandle};
pub use client::Client;
pub use config::{Config, DEFAULT_MAX_HEAD_SIZE, HEAD_BUF_SIZE};
pub use delivery::ChunkStream;
pub use params::RequestBuilder;
pub use response::{Response, StreamingResponse};
