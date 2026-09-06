// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench` command — KV, RPC, disk, chunk, and chunk-IO workloads.
//!
//! Module index only; workload implementations live in `bench/`. Each
//! sub-topic has its own sub-module: `kv`, `rpc`, `disk`, `chunk`, `io`.
//! Shared helpers (`loader`, `metrics`, `result`, `verb`) live directly
//! under `bench/`.

pub mod chunk;
pub mod disk;
pub mod io;
pub mod kv;
pub mod loader;
pub mod metrics;
pub mod result;
pub mod rpc;
pub mod verb;

pub use verb::{run_bench_verb, BenchVerb};
