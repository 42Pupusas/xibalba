//! A worked `Connector` implementation over TCP and rustls.
//!
//! This is a library as well as a binary so its connector can be exercised by
//! tests: the deadline contract in
//! [`Connector::connect`](xibalba_client::connector::Connector::connect) is a
//! claim about behaviour, and a claim reachable only from `main` is a claim
//! nothing can check.

pub mod connector;
pub mod stream;
pub mod tls_config;

pub use connector::TcpConnector;
pub use stream::Stream;
pub use tls_config::RustlsConfig;
