// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only deployment discovery followed by host-generated KV load.
pub mod args;
mod client;
mod collect;
mod counters;
mod dataset;
mod deployment;
mod run;
pub use run::run;
