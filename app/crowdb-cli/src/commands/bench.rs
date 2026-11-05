// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench` command — KV, disk, chunk, chunk-IO, and S3 workloads.
//!
//! Module index only; workload implementations live in `bench/`. Each
//! sub-topic has its own sub-module: `kv`, `disk`, `chunk`, `io`, `s3`.
//! Shared helpers (`loader`, `metrics`, `result`, `verb`) live directly
//! under `bench/`.

pub mod chunk;
mod client;
pub mod disk;
pub mod io;
pub mod loader;
pub mod metrics;
pub mod paxos_kv;
pub mod s3;
pub mod verb;
pub mod workload;

pub use verb::{run_bench_verb, BenchVerb};
