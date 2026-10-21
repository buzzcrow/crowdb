// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolve remote application targets before dispatching logical operations.

use std::{fs, io::Read, process::ExitCode};

use crowdb_console_shared::deployment::target::{self, ClusterTargetBinding, ResolvedClusterTarget};

use crate::Cli;

pub(crate) async fn resolve(cli: &Cli) -> Result<Option<ResolvedClusterTarget>, ExitCode> {
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
            verb: ClusterVerb::Status
                | ClusterVerb::Topology { .. }
                | ClusterVerb::Rack { .. }
                | ClusterVerb::Node { .. }
                | ClusterVerb::DiskGroup { .. }
                | ClusterVerb::Disk { .. }
        } | Domain::Kv {
            verb: KvVerb::Store(_) | KvVerb::Group(_) | KvVerb::Replica(_) | KvVerb::Data(_)
        } | Domain::Chunk { .. }
            | Domain::Bench {
                verb: BenchVerb::Kv(_) | BenchVerb::Diskdb(_) | BenchVerb::Chunkdb(_) | BenchVerb::Chunkio(_)
            }
    );
    if !supported {
        return Err(failure(
            "this operation does not support application target attachment",
        ));
    }
    Ok(())
}

fn load(name: &str) -> Result<ClusterTargetBinding, String> {
    if name.is_empty()
        || name.len() > 48
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    {
        return Err("invalid cluster name".into());
    }
    let path = crowdb_protocol::port::namespace::runtime_root()
        .join("persistent/clusters")
        .join(name)
        .join("target.json");
    let metadata = fs::symlink_metadata(&path)
        .map_err(|_| "selected cluster has no readable target binding".to_owned())?;
    if !metadata.file_type().is_file() || metadata.len() > 32768 {
        return Err("selected cluster target binding is invalid".into());
    }
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|_| "cannot open target binding")?
        .take(32769)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read target binding")?;
    if bytes.len() > 32768 {
        return Err("target binding exceeds its size bound".into());
    }
    let binding: ClusterTargetBinding =
        serde_json::from_slice(&bytes).map_err(|_| "selected cluster target binding is invalid")?;
    if binding.name != name {
        return Err("selected cluster name differs from its binding".into());
    }
    Ok(binding)
}

fn failure(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: resolve cluster target: {error}");
    ExitCode::from(2)
}
