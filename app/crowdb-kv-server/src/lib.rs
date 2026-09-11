// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-kv-server` library — exposes the binary's modules so integration
//! tests under `tests/` can exercise CLI parsing, the management router,
//! and the registry without spawning a process.
//!
//! The binary entry (`main.rs`) imports from this lib via
//! `use crowdb_kv_server::{cli, mgmt, startup, store_registry};`.

pub mod background;
pub mod cli;
pub mod engine_collector;
pub mod mgmt;
pub mod recovery;
pub mod store_registry;
