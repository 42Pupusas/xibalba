//! Shared fixtures for the client's integration tests.
//!
//! Each test binary includes this with `mod support;` and uses part of it, so
//! items unused by a given binary are expected rather than dead.

#![allow(dead_code, reason = "each test binary uses a subset of these fixtures")]

pub(crate) mod client;
pub(crate) mod park;
pub(crate) mod registry;
pub(crate) mod script;
pub(crate) mod scripted;
pub(crate) mod server;
