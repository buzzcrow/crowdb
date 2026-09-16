// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `port-alloc` — flock-coordinated port allocation with bind probes.
//!
//! Moved from the standalone `crowdb-port-alloc` binary into the CLI so
//! cluster bootstrap and E2E fixtures share one binary. Uses the
//! `port_alloc` library directly; no tokio, no RPC, no logging init.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Args;
use crowdb_protocol::port::alloc::{self as port_alloc, PortAllocConfig};
use crowdb_protocol::ServicePort;

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum ServiceArg {
    #[value(name = "kv-mgmt")]
    KvMgmt,
    #[value(name = "kv-listen")]
    KvListen,
    #[value(name = "diskdb-listen")]
    DiskdbListen,
    #[value(name = "diskdb-http")]
    DiskdbHttp,
    #[value(name = "diskdb-rpc")]
    DiskdbRpc,
    #[value(name = "chunkdb-listen")]
    ChunkdbListen,
    #[value(name = "chunkdb-http")]
    ChunkdbHttp,
    #[value(name = "chunkdb-rpc")]
    ChunkdbRpc,
    #[value(name = "diskio-rpc")]
    DiskioRpc,
    #[value(name = "web")]
    Web,
}

impl From<ServiceArg> for ServicePort {
    fn from(arg: ServiceArg) -> Self {
        match arg {
            ServiceArg::KvMgmt => Self::KvServerMgmt,
            ServiceArg::KvListen => Self::KvServerListen,
            ServiceArg::DiskdbListen => Self::DiskdbListen,
            ServiceArg::DiskdbHttp => Self::DiskdbHttp,
            ServiceArg::DiskdbRpc => Self::DiskdbRpc,
            ServiceArg::ChunkdbListen => Self::ChunkdbListen,
            ServiceArg::ChunkdbHttp => Self::ChunkdbHttp,
            ServiceArg::ChunkdbRpc => Self::ChunkdbRpc,
            ServiceArg::DiskioRpc => Self::DiskioRpc,
            ServiceArg::Web => Self::Web,
        }
    }
}

/// Port-alloc arguments. Mirrors the former `crowdb-port-alloc` CLI.
#[derive(Args, Debug)]
pub struct PortAllocArgs {
    /// Workspace root directory (claim file lives under
    /// `<root>/.crowdb-port-alloc/claims`). Default: current directory.
    #[arg(long)]
    pub root: Option<PathBuf>,

    /// Port offset for multi-session isolation. Default: 0.
    #[arg(long, default_value_t = 0)]
    pub offset: u16,

    /// Service type (e.g. "kv-mgmt", "kv-listen", "diskdb-rpc").
    #[arg(long)]
    pub service: Option<ServiceArg>,

    /// Instance index (0-based). Default: 0.
    #[arg(long, default_value_t = 0)]
    pub instance: u16,

    /// Number of consecutive ports to allocate. Default: 1.
    #[arg(long, default_value_t = 1)]
    pub count: u16,

    /// Delete the claim file and exit.
    #[arg(long)]
    pub reset: bool,

    /// Mark a port as tried-and-failed (skip on next probe).
    #[arg(long, value_name = "PORT")]
    pub mark_failed: Option<u16>,
}

/// Run the port-alloc command. Synchronous — no tokio, no RPC, no
/// logging init. Called directly from `main()` before the runtime
/// is built.
pub fn run(args: &PortAllocArgs) -> ExitCode {
    let cfg = match &args.root {
        Some(root) => PortAllocConfig::new(root).with_offset(args.offset),
        None => PortAllocConfig::default().with_offset(args.offset),
    };

    if args.reset {
        match port_alloc::reset_claims(&cfg) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        }
    } else if let Some(port) = args.mark_failed {
        match port_alloc::mark_failed(port, &cfg) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        }
    } else {
        let service = if let Some(s) = &args.service {
            s.clone().into()
        } else {
            eprintln!("error: --service is required (unless --reset or --mark-failed)");
            return ExitCode::FAILURE;
        };
        if args.count <= 1 {
            match port_alloc::alloc_port(service, args.instance, &cfg) {
                Ok(port) => {
                    println!("{port}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::FAILURE
                }
            }
        } else {
            match port_alloc::alloc_port_range(service, args.instance, args.count, &cfg) {
                Ok(ports) => {
                    for port in ports {
                        println!("{port}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}
