// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Mutual SSH preparation; passwords live only in the submitted request.

use crowdb_protocol::mgmt::node::NodeControl;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::config::NodeEntry;
use crate::error::{Error, Result};
use crate::ssh::{Session, SshCreds};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SshNodeIdentity {
    pub discovery_id: String,
    pub public_key: String,
    pub host_key: String,
}

/// # Errors
/// Propagates monitor rejection or transport failures; never sends private keys.
pub async fn local_control(socket: &Path, command: &NodeControl) -> Result<Value> {
    let mut stream = UnixStream::connect(socket).await?;
    let bytes = serde_json::to_vec(command).map_err(config_error)?;
    if bytes.len() > 65536 {
        return Err(Error::Config("control request too large".into()));
    }
    stream.write_all(&bytes).await?;
    stream.shutdown().await?;
    let mut bytes = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(45),
        stream.take(65537).read_to_end(&mut bytes),
    )
    .await
    .map_err(config_error)??;
    if bytes.len() > 65536 {
        return Err(Error::Config("control reply too large".into()));
    }
    let reply: Value = serde_json::from_slice(&bytes).map_err(config_error)?;
    if reply["ok"] != true {
        return Err(Error::Config(
            reply["error"]
                .as_str()
                .unwrap_or("monitor rejected request")
                .into(),
        ));
    }
    Ok(reply["value"].clone())
}

/// # Errors
/// Fails on changed SSH host keys, authentication failure or monitor rejection.
pub async fn remote_control(node: &NodeEntry, command: &NodeControl) -> Result<Value> {
    let mut replies = remote_controls(node, std::slice::from_ref(command)).await?;
    Ok(replies.remove(0))
}

/// Execute ordered commands through one authenticated session, stopping at the
/// first rejection and retaining already completed durable stages for retry.
///
/// # Errors
/// Fails on authentication, transport or monitor rejection.
pub async fn remote_controls(node: &NodeEntry, commands: &[NodeControl]) -> Result<Vec<Value>> {
    let mut session = connect(node).await?;
    let result = async {
        let mut replies = Vec::with_capacity(commands.len());
        for command in commands {
            replies.push(control(&mut session, command).await?);
        }
        Ok(replies)
    }
    .await;
    session.close().await;
    result
}

/// Prepare every peer pair and prove both directions with strict host-key checking.
/// Existing connections and authorization are preserved on a partial failure.
///
/// # Errors
/// Rejects identity mismatch, any SSH failure or a failed mutual access proof.
pub async fn prepare(
    socket: &Path,
    key_path: &Path,
    operation: &str,
    discovery_id: &str,
    candidate: &NodeEntry,
    existing: &[NodeEntry],
) -> Result<SshNodeIdentity> {
    let local: SshNodeIdentity =
        serde_json::from_value(local_control(socket, &NodeControl::Identity).await?).map_err(config_error)?;
    let mut initial = connect(candidate).await?;
    let result: Result<SshNodeIdentity> = async {
        let remote: SshNodeIdentity =
            serde_json::from_value(control(&mut initial, &NodeControl::Identity).await?)
                .map_err(config_error)?;
        if remote.discovery_id != discovery_id {
            return Err(Error::Config("SSH host and candidate UUID differ".into()));
        }
        control(
            &mut initial,
            &NodeControl::InstallKey {
                operation_id: operation.into(),
                public_key: local.public_key.clone(),
            },
        )
        .await?;
        local_control(
            socket,
            &NodeControl::InstallKey {
                operation_id: operation.into(),
                public_key: remote.public_key.clone(),
            },
        )
        .await?;
        Ok(remote)
    }
    .await;
    initial.close().await;
    let remote = result?;
    let mut target = candidate.clone();
    target.ssh_password = None;
    target.ssh_key = Some(key_path.to_string_lossy().into_owned());
    let mut target_session = connect(&target).await?;
    let result = prepare_peers(&mut target_session, &target, operation, &remote, existing).await;
    target_session.close().await;
    result?;
    Ok(remote)
}

async fn prepare_peers(
    target_session: &mut Session,
    target: &NodeEntry,
    operation: &str,
    remote: &SshNodeIdentity,
    existing: &[NodeEntry],
) -> Result<()> {
    let proved: SshNodeIdentity =
        serde_json::from_value(control(target_session, &NodeControl::Identity).await?)
            .map_err(config_error)?;
    if proved.discovery_id != remote.discovery_id {
        return Err(Error::Config(
            "key-authenticated candidate identity differs".into(),
        ));
    }
    for member in existing {
        let mut member = member.clone();
        member.ssh_password = None;
        member.ssh_key = target.ssh_key.clone();
        if member.id == target.id || (member.host == target.host && member.ssh_port == target.ssh_port) {
            continue;
        }
        let started = std::time::Instant::now();
        let mut peer_session = connect(&member).await?;
        let result = prepare_peer(
            &mut peer_session,
            target_session,
            &member,
            target,
            operation,
            remote,
        )
        .await;
        peer_session.close().await;
        result?;
        tracing::debug!(
            node_id = member.id,
            elapsed_ms = started.elapsed().as_millis(),
            "mutual SSH peer preparation completed"
        );
    }
    Ok(())
}

async fn prepare_peer(
    peer_session: &mut Session,
    target_session: &mut Session,
    member: &NodeEntry,
    target: &NodeEntry,
    operation: &str,
    remote: &SshNodeIdentity,
) -> Result<()> {
    let peer: SshNodeIdentity =
        serde_json::from_value(control(peer_session, &NodeControl::Identity).await?).map_err(config_error)?;
    if peer.discovery_id == remote.discovery_id
        && (member.host != target.host || member.ssh_port != target.ssh_port)
    {
        return Err(Error::Config("duplicate SSH discovery identity".into()));
    }
    control(
        peer_session,
        &NodeControl::InstallKey {
            operation_id: operation.into(),
            public_key: remote.public_key.clone(),
        },
    )
    .await?;
    control(
        target_session,
        &NodeControl::InstallKey {
            operation_id: operation.into(),
            public_key: peer.public_key.clone(),
        },
    )
    .await?;
    control(
        peer_session,
        &NodeControl::TrustHost {
            host: target.host.clone(),
            port: target.ssh_port,
            public_key: remote.host_key.clone(),
        },
    )
    .await?;
    control(
        target_session,
        &NodeControl::TrustHost {
            host: member.host.clone(),
            port: member.ssh_port,
            public_key: peer.host_key.clone(),
        },
    )
    .await?;
    let (forward, backward) = tokio::join!(
        verify_pair(peer_session, target, &remote.discovery_id),
        verify_pair(target_session, member, &peer.discovery_id)
    );
    forward?;
    backward?;
    Ok(())
}

async fn verify_pair(session: &mut Session, target: &NodeEntry, identity: &str) -> Result<()> {
    if target.host.is_empty()
        || target
            .host
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !b".:-".contains(&byte))
        || target
            .ssh_user
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !b"_-".contains(&byte))
    {
        return Err(Error::Config("invalid SSH destination".into()));
    }
    let request = serde_json::to_string(&NodeControl::Identity).map_err(config_error)?;
    let command = format!("ssh -i /opt/crowdb/data/ssh/id_ed25519 -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/opt/crowdb/data/ssh/known_hosts -p {} {} LD_LIBRARY_PATH=/opt/crowdb/lib /opt/crowdb/bin/crowdb-monitor control --json {}", target.ssh_port, quote(&format!("{}@{}", target.ssh_user, target.host)), quote(&quote(&request)));
    let output = tokio::time::timeout(Duration::from_secs(20), session.exec(&command))
        .await
        .map_err(config_error)??;
    if !output.success() {
        return Err(Error::Config(format!(
            "mutual SSH verification failed: {}",
            output.stderr_str()
        )));
    }
    let remote: SshNodeIdentity = serde_json::from_slice(&output.stdout).map_err(config_error)?;
    if remote.discovery_id != identity {
        return Err(Error::Config("mutual SSH identity differs".into()));
    }
    Ok(())
}

async fn control(session: &mut Session, request: &NodeControl) -> Result<Value> {
    let json = serde_json::to_string(request).map_err(config_error)?;
    let command = format!(
        "LD_LIBRARY_PATH=/opt/crowdb/lib /opt/crowdb/bin/crowdb-monitor control --json {}",
        quote(&json)
    );
    let output = tokio::time::timeout(Duration::from_secs(45), session.exec(&command))
        .await
        .map_err(config_error)??;
    if !output.success() {
        return Err(Error::Config(format!(
            "monitor SSH command failed: {}",
            output.stderr_str()
        )));
    }
    if output.stdout.len() > 65536 {
        return Err(Error::Config("monitor reply too large".into()));
    }
    serde_json::from_slice(&output.stdout).map_err(config_error)
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

async fn connect(node: &NodeEntry) -> Result<Session> {
    let credentials = SshCreds::resolve(node)?;
    tokio::time::timeout(Duration::from_secs(10), Session::connect(node, &credentials))
        .await
        .map_err(|error| Error::UpstreamRpc {
            node_id: node.id.to_string(),
            status: format!("SSH connection/authentication exceeded 10 seconds: {error}"),
        })?
}
fn config_error(error: impl std::fmt::Display) -> Error {
    Error::Config(error.to_string())
}
