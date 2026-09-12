// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-chunk-kv-server` process entry point.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use crowdb_chunk_kv_server::{
    management_router, ChunkKvServerConfig, ChunkKvService, ChunkKvStorage, ManagementState,
};
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(
    name = "crowdb-chunk-kv-server",
    about = "CROWDB range-partitioned chunk-backed KV server"
)]
struct Cli {
    /// Server configuration file.
    #[arg(long)]
    config: PathBuf,

    /// HTTP management listen address override.
    #[arg(long)]
    http_addr: Option<String>,

    /// Data RPC listen address override.
    #[arg(long)]
    rpc_addr: Option<String>,

    /// Log directory.
    #[arg(long, default_value = "log")]
    log_dir: String,

    /// Also emit warning and error logs to the console.
    #[arg(short = 'l', long)]
    log: bool,

    /// Maximum log file size in MiB before rotation.
    #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILE_MB)]
    log_max_file_mb: usize,

    /// Number of rotated log files to retain.
    #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILES)]
    log_max_files: usize,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let _log_guards = if args.log {
        crowdb_common::logging::init_file_and_console_logging_split(
            &args.log_dir,
            "crowdb-chunk-kv-server",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
            "warn",
        )
    } else {
        crowdb_common::logging::init_file_logging(
            &args.log_dir,
            "crowdb-chunk-kv-server",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
        )
    }
    .expect("failed to initialize chunk KV server logging");
    crowdb_tree_ffi::ct_init_logging(
        &args.log_dir,
        "info",
        args.log_max_file_mb,
        args.log_max_files,
        "crowdb-chunk-kv-server-tree",
    );
    crowdb_rpc_ffi::init_logging(
        &args.log_dir,
        "info",
        args.log_max_file_mb,
        args.log_max_files,
        "crowdb-chunk-kv-server-rpc",
    );

    let mut config = match ChunkKvServerConfig::load(&args.config) {
        Ok(config) => config,
        Err(error) => {
            error!(path = %args.config.display(), %error, "failed to load configuration");
            return;
        }
    };
    if let Some(http_addr) = args.http_addr {
        config.http_listen_addr = http_addr;
    }
    if let Some(rpc_addr) = args.rpc_addr {
        config.rpc_listen_addr = rpc_addr;
    }
    if let Err(error) = config.validate() {
        error!(%error, "configuration overrides are invalid");
        return;
    }

    let http_addr: SocketAddr = config
        .http_listen_addr
        .parse()
        .expect("validated HTTP listen address");
    let rpc_addr: SocketAddr = config
        .rpc_listen_addr
        .parse()
        .expect("validated RPC listen address");
    info!(
        instance_id = config.instance_id,
        %http_addr,
        %rpc_addr,
        "crowdb-chunk-kv-server starting"
    );

    let _storage = match ChunkKvStorage::connect(&config).await {
        Ok(storage) => storage,
        Err(error) => {
            error!(%error, "failed to connect production chunk storage");
            return;
        }
    };
    let service = match ChunkKvService::new(config.instance_id, config.max_hosted_partitions) {
        Ok(service) => Arc::new(service),
        Err(error) => {
            error!(%error, "failed to initialize chunk KV service");
            return;
        }
    };

    let listener = match tokio::net::TcpListener::bind(http_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            error!(%http_addr, %error, "HTTP management bind failed");
            return;
        }
    };
    info!(%http_addr, "HTTP management server listening");
    warn!(%rpc_addr, "data RPC listener is not implemented; readiness remains fenced");

    let shutdown_service = Arc::clone(&service);
    let app = management_router(ManagementState::new(service));
    if let Err(error) = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            shutdown_service.begin_drain();
            info!("chunk KV service admission drained");
        })
        .await
    {
        error!(%error, "HTTP management server failed");
    }
}

async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                warn!(%error, "SIGINT handler failed");
            }
        }
        _ = terminate.recv() => {}
    }
    info!("received shutdown signal");
}
