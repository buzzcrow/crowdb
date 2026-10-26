// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Test-only echo measurement. The fixture owns server startup and shutdown.

mod args;
mod workload;

use clap::Parser;
use std::{path::PathBuf, process::ExitCode};

#[derive(Parser)]
#[command(about = "CROWDB RPC transport test client")]
struct Options {
    #[arg(long)]
    log_dir: PathBuf,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    rdma: bool,
    #[command(flatten)]
    workload: args::RpcArgs,
}

#[tokio::main]
async fn main() -> ExitCode {
    let options = Options::parse();
    if let Err(error) = std::fs::create_dir_all(&options.log_dir) {
        eprintln!("cannot create test log directory: {error}");
        return ExitCode::from(2);
    }
    let _guards = crowdb_common::logging::init_file_and_console_logging_split(
        &options.log_dir,
        "rpc-test",
        30,
        1,
        "warn",
        "warn",
    )
    .ok();
    crowdb_rpc_ffi::init_logging(options.log_dir.to_str().unwrap_or(""), "warn", 30, 1, "rpc-test");
    workload::run(&options.log_dir, options.workload, options.rdma, options.json).await
}
