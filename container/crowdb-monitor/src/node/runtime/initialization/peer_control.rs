// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Use the peer keys pinned by admission, independently of Console's TOFU store.

use std::{path::Path, time::Duration};

use crowdb_console_shared::config::NodeEntry;
use crowdb_protocol::mgmt::node::NodeControl;
use tokio::process::Command;

use super::Result;

pub(super) async fn execute(root: &Path, node: &NodeEntry, request: &NodeControl) -> Result<()> {
    if node.host.is_empty()
        || node.host.starts_with('-')
        || node
            .host
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !b".:-".contains(&byte))
        || node.ssh_user.is_empty()
        || node
            .ssh_user
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !b"_-".contains(&byte))
    {
        return Err("invalid peer SSH destination".into());
    }
    let json = serde_json::to_string(request)?;
    let command = format!(
        "LD_LIBRARY_PATH=/opt/crowdb/lib /opt/crowdb/bin/crowdb-monitor control --json '{}'",
        json.replace('\'', "'\\''")
    );
    let output = tokio::time::timeout(
        Duration::from_secs(45),
        Command::new("ssh")
            .kill_on_drop(true)
            .arg("-i")
            .arg(root.join("ssh/id_ed25519"))
            .args([
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "StrictHostKeyChecking=yes",
            ])
            .arg("-o")
            .arg(format!(
                "UserKnownHostsFile={}",
                root.join("ssh/known_hosts").display()
            ))
            .args([
                "-o",
                "GlobalKnownHostsFile=/dev/null",
                "-o",
                "IdentitiesOnly=yes",
                "-p",
            ])
            .arg(node.ssh_port.to_string())
            .arg(format!("{}@{}", node.ssh_user, node.host))
            .arg(command)
            .output(),
    )
    .await??;
    if !output.status.success() {
        return Err(format!(
            "peer monitor control failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    if output.stdout.len() > 65536 {
        return Err("peer monitor reply too large".into());
    }
    serde_json::from_slice::<serde_json::Value>(&output.stdout)?;
    Ok(())
}
