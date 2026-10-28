// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cross-cutting utilities shared by the `cluster`, `paxos`, and `rpc`
//! modules: static configuration profiles,
//! shutdown / multi-step operation reporting, monotonic-time helpers,
//! and tracing-subscriber initialization.

pub mod config;
pub mod logging;
pub(crate) mod report;
pub(crate) mod time;

pub use report::OperationReport;
