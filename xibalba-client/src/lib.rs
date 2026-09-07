//! A blocking and a background-reader HTTP/1.1 client over [`xibalba_proto`].
//!
//! - [`client::Client`] — one connection, caller-driven, on the calling
//!   thread. Start with [`Client::connect`] and [`Client::get`].
//! - [`async_client::AsyncClient`] — the same client on a dedicated reader
//!   thread, responses delivered as chunks over ring buffers.
//!
//! # Buffering the whole body
//!
//! [`Response::text`] consumes the response and returns the body as a
//! `String`, so read the status and headers first.
//!
//! ```
//! # use std::io::{Read, Write};
//! # use std::net::TcpListener;
//! use xibalba_client::{Client, PlainConnector};
//! use xibalba_client::proto::status::StatusCode;
//!
//! # let listener = TcpListener::bind("127.0.0.1:0")?;
//! # let port = listener.local_addr()?.port();
//! # std::thread::spawn(move || {
//! #     let (mut stream, _) = listener.accept().unwrap();
//! #     let mut buf = [0u8; 1024];
//! #     let _ = stream.read(&mut buf).unwrap();
//! #     stream.write_all(
//! #         b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\nhello",
//! #     ).unwrap();
//! # });
//! let url = format!("http://127.0.0.1:{port}/");
//! let mut client = Client::<PlainConnector>::connect_default(url.as_bytes(), ())?;
//! let response = client.get(b"/")?;
//!
//! assert_eq!(response.status, StatusCode::OK);
//!
//! // `headers()` is a method, and borrows; `text()` takes the response by value.
//! let content_type = response
//!     .headers()
//!     .find(|(name, _)| name.eq_ignore_ascii_case(b"content-type"))
//!     .map(|(_, value)| value.to_vec());
//! assert_eq!(content_type.as_deref(), Some(&b"text/plain"[..]));
//!
//! assert_eq!(response.text()?, "hello");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Streaming the body
//!
//! `body` is a field implementing [`std::io::Read`], not a method. Reading it
//! incrementally never buffers the whole body in memory.
//!
//! ```
//! # use std::io::{Read, Write};
//! # use std::net::TcpListener;
//! use xibalba_client::{Client, PlainConnector};
//!
//! # let listener = TcpListener::bind("127.0.0.1:0")?;
//! # let port = listener.local_addr()?.port();
//! # std::thread::spawn(move || {
//! #     let (mut stream, _) = listener.accept().unwrap();
//! #     let mut buf = [0u8; 1024];
//! #     let _ = stream.read(&mut buf).unwrap();
//! #     stream.write_all(
//! #         b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
//! #     ).unwrap();
//! # });
//! let url = format!("http://127.0.0.1:{port}/");
//! let mut client = Client::<PlainConnector>::connect_default(url.as_bytes(), ())?;
//! let mut response = client.get(b"/")?;
//!
//! let mut buf = [0u8; 8192];
//! let mut received = Vec::new();
//! loop {
//!     let n = response.body.read(&mut buf)?;
//!     if n == 0 {
//!         break;
//!     }
//!     received.extend_from_slice(&buf[..n]);
//! }
//!
//! assert_eq!(received, b"hello world");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod async_client;
pub mod body;
pub mod client;
pub mod config;
pub mod connector;
pub mod deadline;
pub mod dial;
pub mod interrupt;
pub mod origin;
pub mod plain;
pub mod reference;
pub mod response;

mod admission;
mod control;
mod delivery;
mod params;
mod reader;
mod redirect;
mod reuse;
mod silence;

pub use xibalba_proto as proto;

pub use admission::DEFAULT_MAX_OUTSTANDING;
pub use async_client::{AsyncClient, Chunk, StreamHandle};
pub use client::Client;
pub use config::{Config, DEFAULT_MAX_HEAD_SIZE, HEAD_BUF_SIZE};
pub use deadline::{Deadline, TimeLeft};
pub use delivery::ChunkStream;
pub use interrupt::{Interrupt, InterruptibleStream, NeverCancelled};
pub use origin::Origin;
pub use params::RequestBuilder;
pub use plain::{PlainConnector, PlainStream};
pub use response::{Response, StreamingResponse};
