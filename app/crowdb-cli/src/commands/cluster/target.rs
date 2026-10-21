// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolve remote application targets before dispatching logical operations.

use std::process::ExitCode;

use crowdb_console_shared::deployment::target::{self, ClusterTargetBinding, ResolvedClusterTarget};

use crate::Cli;

pub(crate) async fn resolve(cli: &Cli) -> Result<Option<ResolvedClusterTarget>, ExitCode> {
    if matches!(
        cli.command,
        crate::Domain::Cluster {
            verb: super::ClusterVerb::Status { .. }
                | super::ClusterVerb::Check { .. }
                | super::ClusterVerb::Start(_)
                | super::ClusterVerb::Stop
        }
    ) {
        return Ok(None);
    }
    if cli.cluster.is_none() && cli.management_endpoints.is_empty() {
        return Ok(None);
    }
    validate_command(&cli.command)?;
    let binding = cli.cluster.as_deref().map(load).transpose().map_err(failure)?;
    target::resolve(binding.as_ref(), &cli.management_endpoints, None)
        .await
        .map(Some)
        .map_err(failure)
}

fn validate_command(command: &crate::Domain) -> Result<(), ExitCode> {
    use crate::{
        commands::{BenchVerb, ClusterVerb},
        Domain, KvVerb,
    };
    let supported = matches!(
        command,
        Domain::Cluster {
            verb: ClusterVerb::Status { .. }
                | ClusterVerb::Check { .. }
                | ClusterVerb::Topology { .. }
                | ClusterVerb::Rack { .. }
                | ClusterVerb::Node { .. }
                | ClusterVerb::DiskGroup { .. }
                | ClusterVerb::Disk { .. }
        } | Domain::Kv {
            verb: KvVerb::Store(_) | KvVerb::Group(_) | KvVerb::Replica(_) | KvVerb::Data(_)
        } | Domain::Chunk { .. }
            | Domain::S3 { .. }
            | Domain::Bench {
                verb: BenchVerb::Kv(_)
                    | BenchVerb::Diskdb(_)
                    | BenchVerb::Chunkdb(_)
                    | BenchVerb::Chunkio(_)
                    | BenchVerb::S3(_)
            }
    );
    if !supported {
        return Err(failure(
            "this operation does not support application target attachment",
        ));
    }
    Ok(())
}

pub(super) fn load(name: &str) -> Result<ClusterTargetBinding, String> {
    super::local::load_target(name)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "selected cluster has no readable target binding".into())
}

fn failure(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: resolve cluster target: {error}");
    ExitCode::from(2)
}
