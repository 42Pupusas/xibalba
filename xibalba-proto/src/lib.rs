//! Zero-copy HTTP/1.1 parsing and serialization, with no I/O of its own.
//!
//! Every parsed type borrows the caller's buffer rather than allocating, so
//! the buffer must outlive whatever was parsed out of it. The caller also owns
//! the header array, which bounds header count without a heap allocation.
//!
//! # Parsing a URL
//!
//! ```
//! use xibalba_proto::url::Url;
//! use xibalba_proto::scheme::Scheme;
//!
//! let url = Url::parse(b"https://example.com:8443/api?page=2")?;
//!
//! assert_eq!(url.scheme, Scheme::Https);
//! assert_eq!(url.host, b"example.com");
//! assert_eq!(url.effective_port(), 8443);
//! assert_eq!(url.path, b"/api");
//! # Ok::<(), xibalba_proto::error::Error>(())
//! ```
//!
//! # Parsing a response head
//!
//! [`ResponseHead::parse`] fills a caller-owned header array and reports how
//! many bytes it consumed, so the remainder of the buffer is the start of the
//! body. Read the headers back through [`ResponseHead::headers`], which pairs
//! the count with the array it was parsed into.
//!
//! ```
//! use xibalba_proto::header::Header;
//! use xibalba_proto::response::ResponseHead;
//! use xibalba_proto::status::StatusCode;
//!
//! let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi";
//! // `Header` borrows, so it is not `Copy`; the array repeat needs a const block.
//! let mut headers = [const { Header::empty() }; 8];
//! let (head, consumed) = ResponseHead::parse(raw, &mut headers)?;
//!
//! assert_eq!(head.status, StatusCode::OK);
//! assert_eq!(&raw[consumed..], b"hi");
//!
//! let parsed = head.headers(&headers)?;
//! assert_eq!(parsed.len(), 1);
//! assert_eq!(parsed[0].value, b"2");
//! # Ok::<(), xibalba_proto::error::Error>(())
//! ```
//!
//! # Incomplete input
//!
//! A head split across reads is [`ParseError::Incomplete`], which is distinct
//! from a malformed head. Retry with more bytes rather than treating it as an
//! error.
//!
//! ```
//! use xibalba_proto::error::{Error, ParseError};
//! use xibalba_proto::header::Header;
//! use xibalba_proto::response::ResponseHead;
//!
//! let mut headers = [const { Header::empty() }; 8];
//! let partial = b"HTTP/1.1 200 OK\r\nContent-Len";
//!
//! assert!(matches!(
//!     ResponseHead::parse(partial, &mut headers),
//!     Err(Error::Parse(ParseError::Incomplete)),
//! ));
//! ```
//!
//! [`ResponseHead::parse`]: response::ResponseHead::parse
//! [`ResponseHead::headers`]: response::ResponseHead::headers
//! [`ParseError::Incomplete`]: error::ParseError::Incomplete

pub mod bytes;
pub mod coding;
pub mod error;

pub use method::Token;
pub mod header;
pub mod method;
pub mod request;
pub mod response;
pub mod scheme;
pub mod status;
pub mod url;
pub mod version;
