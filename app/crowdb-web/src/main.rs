// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-web` binary entrypoint.

use std::net::SocketAddr;

use clap::Parser;
use crowdb_common::logging::init_file_and_console_logging_split;
use crowdb_console_shared::config::web::{LaunchRegistry, WebMode, WebProcessConfig};
use crowdb_protocol::WEB_BASE;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "crowdb-web")]
struct Args {
    /// Bind address for the web server (default: 0.0.0.0)
    #[arg(long, conflicts_with = "config")]
    bind: Option<String>,

    /// Port for the web server (default: 14000)
    #[arg(long, conflicts_with = "config", value_parser = clap::value_parser!(u16).range(1..))]
    port: Option<u16>,

    /// Use an in-memory registry instead of the persisted console config.
    #[arg(long, conflicts_with = "config")]
    test_mode: bool,

    /// Versioned web process configuration.
    #[arg(long, value_name = "PATH")]
    config: Option<std::path::PathBuf>,

    /// Optional launch-only registry for bare-metal deployments.
    #[arg(long, value_name = "PATH", requires = "config")]
    registry: Option<std::path::PathBuf>,

    /// Load the registry without reconciling service processes at startup.
    #[arg(long, conflicts_with = "config")]
    skip_startup_restore: bool,

    /// Log directory. Default: ~/.crowdb-kv/log.
    #[arg(long, conflicts_with = "config")]
    log_dir: Option<std::path::PathBuf>,

    /// Log level for both Rust and C++ stacks. Default: "info"
    /// (or derived from `RUST_LOG`).
    #[arg(long)]
    log_level: Option<String>,

    /// Max log file size in MiB before rotation. Default: 30.
    #[arg(long, conflicts_with = "config")]
    log_max_file_mb: Option<usize>,

    /// Number of rotated log files to keep. Default: 5.
    #[arg(long, conflicts_with = "config")]
    log_max_files: Option<usize>,

    /// Also print logs to console (in addition to file logging).
    #[arg(short = 'l', long)]
    log: bool,

    /// Mirror C++ log lines at this level or above to stderr.
    /// Default: "warn" (mirrors warn+error to stderr).
    #[arg(long)]
    log_stderr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    if args.config.is_none() && !args.test_mode {
        return Err("crowdb-web requires a versioned --config outside test mode".into());
    }
    let process_config = args.config.as_deref().map(WebProcessConfig::load).transpose()?;
    if args.registry.is_some()
        && process_config
            .as_ref()
            .is_some_and(|config| config.mode == WebMode::Docker)
    {
        return Err("docker web does not accept --registry".into());
    }
    let launch_registry = args.registry.as_deref().map(LaunchRegistry::load).transpose()?;
    let _log_guards = init_logging(&args, process_config.as_ref())?;

    let bind = process_config.as_ref().map_or_else(
        || args.bind.as_deref().unwrap_or("0.0.0.0"),
        |config| config.bind.as_str(),
    );
    let port = process_config
        .as_ref()
        .map_or_else(|| args.port.unwrap_or(WEB_BASE), |config| config.port);
    let addr: SocketAddr = format!("{bind}:{port}").parse()?;
    info!(%addr, "crowdb-web starting");

    let mut state = crowdb_web::AppState::default().with_test_mode(args.test_mode);
    if let Some(config) = process_config {
        state = state.with_process_config(&config);
        state = state.with_management_token(std::env::var("CROWDB_ICEBERG_MANAGE_TOKEN")?)?;
    }
    if let Some(path) = &args.registry {
        state = state.with_launch_registry(path.clone())?;
        let started = state.start_configured_services().await?;
        info!(started, "reconciled configured service launches");
    }
    tracing::info!(
        servers = 0,
        launches = launch_registry
            .as_ref()
            .map_or(0, |registry| registry.launches.len()),
        "loaded web startup configuration"
    );
    if !args.skip_startup_restore && !state.managed_mode {
        crowdb_web::mgmt::startup_topology_check(&state).await;
    }

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, crowdb_web::router(state)).await?;
    Ok(())
}

fn init_logging(
    args: &Args,
    process_config: Option<&WebProcessConfig>,
) -> Result<crowdb_common::logging::LogGuards, String> {
    let log_max_file_mb = process_config.map_or_else(
        || {
            args.log_max_file_mb
                .unwrap_or(crowdb_common::logging::DEFAULT_LOG_MAX_FILE_MB)
        },
        |config| config.log_max_file_mb,
    );
    let log_max_files = process_config.map_or_else(
        || {
            args.log_max_files
                .unwrap_or(crowdb_common::logging::DEFAULT_LOG_MAX_FILES)
        },
        |config| config.log_max_files,
    );
    let log_dir = process_config
        .map(|config| config.log_dir.clone())
        .or(args.log_dir.clone())
        .unwrap_or_else(|| {
            crowdb_protocol::port::namespace::runtime_root()
                .join("persistent")
                .join("console")
                .join("log")
        });
    let guards = if args.log {
        init_file_and_console_logging_split(
            &log_dir,
            "console-web",
            log_max_file_mb,
            log_max_files,
            "info",
            "warn",
        )?
    } else {
        crowdb_common::logging::init_file_logging(
            &log_dir,
            "console-web",
            log_max_file_mb,
            log_max_files,
            "info",
        )?
    };
    let cpp_level = args
        .log_level
        .clone()
        .unwrap_or_else(|| crowdb_common::logging::cpp_level_from_rust_log("info"));
    crowdb_rpc_ffi::init_logging(
        &log_dir.to_string_lossy(),
        &cpp_level,
        log_max_file_mb,
        log_max_files,
        "crowdb-web-rpc",
    );
    let stderr_level = args.log_stderr.as_deref().unwrap_or("warn");
    if stderr_level != "off" {
        crowdb_rpc_ffi::add_log_stderr(stderr_level);
    }
    Ok(guards)
}
