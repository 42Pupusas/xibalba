//! End-to-end tests against a loopback server.
//!
//! One binary, one module per concern. Shared servers and client constructors
//! live in [`support`]; a module here should read as the behaviour it covers,
//! not as a pile of socket setup.

#[path = "../support/mod.rs"]
mod support;

mod adversarial;
mod async_client;
mod audit_regressions;
mod builder;
mod host_header;
mod redirects;
mod request_body;
mod responses;
mod size_limits;
mod streaming;
mod timeouts;
