// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit test transport parameters; no application deployment inputs.

use clap::ValueEnum;

#[derive(clap::Args, Debug)]
pub struct RpcArgs {
    /// Emit the structured workload result on stdout.
    #[arg(long)]
    pub json: bool,
    /// Run duration in seconds.
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    /// Number of loader coroutines / tasks.
    #[arg(long, default_value_t = 1)]
    pub loader_num: usize,
    /// Number of client connections to the fb-server.
    #[arg(long, default_value_t = 1)]
    pub connections: usize,
    /// Echo payload size in bytes.
    #[arg(long, default_value_t = 128)]
    pub value_size: usize,
    /// Client transport I/O engines (must match the fb-server).
    #[arg(long, default_value_t = 1)]
    pub io_engines: u32,
    /// Client transport I/O worker threads (must match the fb-server).
    #[arg(long, default_value_t = 1)]
    pub io_workers: u32,
    /// Concurrency model: `coroutine` (C++ coroutines) or `tokio`.
    #[arg(long, value_enum, default_value_t = BenchMode::Coroutine)]
    pub mode: BenchMode,
    /// fb-server port to connect to.
    #[arg(long)]
    pub server_port: u16,
    /// Enable TCP Nagle (coalesce small frames per writev).
    #[arg(long, default_value_t = false)]
    pub enable_nagle: bool,
    /// Metrics flush interval in seconds. 0 disables the metrics log.
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum BenchMode {
    Coroutine,
    Tokio,
}
