#![warn(clippy::pedantic)]
#![warn(clippy::perf)]
#![warn(clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod body;
pub mod client;
pub mod connection;
pub mod error;
pub mod header;
pub mod method;
pub mod request;
pub mod response;
pub mod scheme;
pub mod status;
pub mod stream;
pub mod url;
pub mod version;
