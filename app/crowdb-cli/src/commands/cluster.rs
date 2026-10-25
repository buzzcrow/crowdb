// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Common OCI cluster lifecycle, observation and hardware metadata.

pub mod hardware;
mod local;
mod status;
pub(crate) mod target;

pub(crate) use hardware::{
    run_disk_group_verb, run_disk_verb, run_node_verb, run_rack_verb, DiskGroupVerb, DiskVerb, NodeVerb,
    RackVerb,
};

use std::process::ExitCode;

use clap::Subcommand;

use crate::commands::authority_context;
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ClusterVerb {
    /// Start or resume a named OCI cluster; one node is the default.
    Start(local::StartArgs),
    /// Stop the selected cluster's owned containers, preserving its data.
    Stop,
    /// Observe current application identity, initialization and readiness.
    Status {
        #[arg(long)]
        json: bool,
        /// Observation budget in seconds.
        #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
        timeout: u64,
    },
    /// Check whole-cluster readiness; exit 1 when the contract is unmet.
    Check {
        #[arg(long)]
        json: bool,
        /// Maximum wait in seconds; zero observes once.
        #[arg(long, default_value_t = 0)]
        wait: u64,
        /// Maximum duration of one observation request in seconds.
        #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
        timeout: u64,
    },
    /// Show the topology view from a node's `/topology` endpoint.
    Topology {
        #[arg(short = 'n', long)]
        node: String,
    },
    /// Hardware: rack management.
    Rack {
        #[command(subcommand)]
        verb: RackVerb,
    },
    /// Hardware: node management.
    Node {
        #[command(subcommand)]
        verb: NodeVerb,
    },
    /// Hardware: disk-group management.
    #[command(name = "disk-group")]
    DiskGroup {
        #[command(subcommand)]
        verb: DiskGroupVerb,
    },
    /// Hardware: disk management.
    Disk {
        #[command(subcommand)]
        verb: DiskVerb,
    },
}

pub async fn run_cluster_verb(cli: &Cli, verb: ClusterVerb) -> ExitCode {
    match verb {
        ClusterVerb::Start(args) => local::start(cli, &args).await,
        ClusterVerb::Stop => local::stop(cli).await,
        ClusterVerb::Status { json, timeout } => status::run(cli, json, false, 0, timeout).await,
        ClusterVerb::Check { json, wait, timeout } => status::run(cli, json, true, wait, timeout).await,
        ClusterVerb::Topology { node } => {
            let ctx = match authority_context(cli).await {
                Ok(c) => c,
                Err(c) => return c,
            };
            let node_id: u64 = match node.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            match crowdb_console_shared::ops::cluster::topology(&ctx, node_id).await {
                Ok(stores) => {
                    for s in &stores {
                        println!(
                            "store {} listen={}",
                            s.store_id,
                            s.listen_addr.as_deref().unwrap_or("-")
                        );
                        for g in &s.groups {
                            println!("  group {} leader={}", g.group_id, g.local_replica_id);
                        }
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster topology: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Rack { verb } => run_rack_verb(cli, verb).await,
        ClusterVerb::Node { verb } => run_node_verb(cli, verb).await,
        ClusterVerb::DiskGroup { verb } => run_disk_group_verb(cli, verb).await,
        ClusterVerb::Disk { verb } => run_disk_verb(cli, verb).await,
    }
}
