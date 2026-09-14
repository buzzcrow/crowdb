// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production chunk-stream benchmark entry point.

#[path = "production_bounds/config.rs"]
mod config;
#[path = "production_bounds/result.rs"]
mod result;
#[path = "production_bounds/workload.rs"]
mod workload;

use std::process::ExitCode;
use std::sync::Arc;

use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, SmallWritePolicy};
use crowdb_chunk_stream::{ProductionStreamRuntime, StreamConfig};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};

use config::BenchConfig;

fn main() -> ExitCode {
    if cfg!(debug_assertions) {
        return ExitCode::SUCCESS;
    }
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.iter().any(|argument| argument == "--test") {
        return ExitCode::SUCCESS;
    }
    let config = match BenchConfig::parse(arguments.into_iter()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("invalid chunk-stream benchmark arguments: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build benchmark runtime");
    runtime.block_on(run(config))
}

async fn run(config: BenchConfig) -> ExitCode {
    let chunk_io = match ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: vec![config.management_seed.clone()],
        diskio_connections_per_endpoint: config.diskio_connections,
        diskio_rpc_workers: config.diskio_rpc_workers,
        small_write: SmallWritePolicy::default(),
    })
    .await
    {
        Ok(client) => client,
        Err(error) => {
            eprintln!("chunk-stream benchmark discovery failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![config
        .management_seed
        .clone()])));
    let stream_runtime = match ProductionStreamRuntime::new(
        kv,
        &chunk_io,
        30_000,
        ChunkReadPolicy::default(),
        StreamConfig::default(),
    ) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("chunk-stream benchmark assembly failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    match workload::run(&stream_runtime, &config).await {
        Ok(result) => {
            println!("{result}");
            if result.errors == 0 && result.operations > 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            eprintln!("chunk-stream benchmark failed: {error}");
            ExitCode::FAILURE
        }
    }
}
