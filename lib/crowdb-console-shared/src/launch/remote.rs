// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! SSH process control with referenced credentials and checked process identity.

use std::path::Path;
use std::time::Duration;

use crate::config::{web::LaunchRecord, NodeEntry};
use crate::error::{Error, Result};
use crate::ssh::{Session, SshCreds};

use super::runtime::{self, ProcessIdentity};

async fn connect(launch: &LaunchRecord, credential_root: &Path) -> Result<Session> {
    let node = NodeEntry {
        id: launch.node_id,
        rack_id: 0,
        host: launch.host.clone(),
        ssh_port: launch.ssh_port,
        ssh_user: launch.ssh_user.clone().unwrap_or_default(),
        ssh_key: launch
            .ssh_credential_ref
            .as_ref()
            .map(|reference| credential_root.join(reference).to_string_lossy().into_owned()),
        ssh_password: None,
        ssh_credential_ref: None,
    };
    Session::connect(&node, &SshCreds::resolve(&node)?).await
}

pub(super) async fn identity(
    launch: &LaunchRecord,
    credential_root: &Path,
    pid: u32,
) -> Result<Option<ProcessIdentity>> {
    read_identity(&mut connect(launch, credential_root).await?, pid).await
}

async fn read_identity(session: &mut Session, pid: u32) -> Result<Option<ProcessIdentity>> {
    let output = session
        .exec(&format!(
            "if test -r /proc/{pid}/stat; then cat /proc/{pid}/stat; else exit 3; fi"
        ))
        .await?;
    if output.exit == Some(3) {
        return Ok(None);
    }
    if !output.success() {
        return Err(Error::Config("cannot read remote process identity".into()));
    }
    runtime::parse_identity(pid, &output.stdout_str())
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub(super) async fn start(
    root: &Path,
    launch: &LaunchRecord,
    credential_root: &Path,
) -> Result<ProcessIdentity> {
    let mut session = connect(launch, credential_root).await?;
    let log_dir = launch.workspace.join("log");
    let log_path = log_dir.join(format!("{}.launch.log", launch.service_id));
    let args = launch
        .command_args()
        .iter()
        .map(|arg| quote(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let command = format!(
        "mkdir -p {} && cd {} && {{ nohup setsid {} {} </dev/null >>{} 2>&1 & echo $!; }}",
        quote(&log_dir.to_string_lossy()),
        quote(&launch.workspace.to_string_lossy()),
        quote(&launch.binary_path.to_string_lossy()),
        args,
        quote(&log_path.to_string_lossy())
    );
    let output = session.exec(&command).await?;
    if !output.success() {
        return Err(Error::Config("remote launch command failed".into()));
    }
    let pid = output
        .stdout_str()
        .trim()
        .parse::<u32>()
        .ok()
        .filter(|pid| *pid > 0)
        .ok_or_else(|| Error::Config("remote launch did not return a process identity".into()))?;
    let identity = read_identity(&mut session, pid)
        .await?
        .ok_or_else(|| Error::Config("remote launch exited before identity capture".into()))?;
    let result = async {
        wait_ready(&mut session, launch, identity).await?;
        runtime::save(root, launch, identity)?;
        Ok(identity)
    }
    .await;
    if let Err(original) = &result {
        if let Err(cleanup) = stop_session(&mut session, identity).await {
            return Err(Error::UpstreamRpc {
                node_id: launch.node_id.to_string(),
                status: format!("{original}; launch cleanup incomplete: {cleanup}"),
            });
        }
    }
    result
}

async fn wait_ready(session: &mut Session, launch: &LaunchRecord, identity: ProcessIdentity) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .map_err(|error| Error::Config(error.to_string()))?;
    let started = tokio::time::Instant::now();
    loop {
        if read_identity(session, identity.pid).await? != Some(identity) {
            return Err(Error::Config("remote launch exited before readiness".into()));
        }
        let ready = match &launch.readiness_url {
            Some(url) => client
                .get(url)
                .send()
                .await
                .is_ok_and(|reply| reply.status().is_success()),
            None => started.elapsed() >= Duration::from_millis(200),
        };
        if ready {
            return Ok(());
        }
        if started.elapsed() >= Duration::from_secs(30) {
            return Err(Error::NodeUnreachable {
                node_id: launch.node_id.to_string(),
                reason: "remote service did not become ready".into(),
            });
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) async fn stop(
    launch: &LaunchRecord,
    credential_root: &Path,
    identity: ProcessIdentity,
) -> Result<()> {
    stop_session(&mut connect(launch, credential_root).await?, identity).await
}

async fn stop_session(session: &mut Session, identity: ProcessIdentity) -> Result<()> {
    for (signal, budget) in [("TERM", 15), ("KILL", 2)] {
        if read_identity(session, identity.pid).await? != Some(identity) {
            return Ok(());
        }
        session.exec(&format!("kill -{signal} {}", identity.pid)).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(budget);
        while tokio::time::Instant::now() < deadline {
            if read_identity(session, identity.pid).await? != Some(identity) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    Err(Error::Config("remote process did not stop".into()))
}
