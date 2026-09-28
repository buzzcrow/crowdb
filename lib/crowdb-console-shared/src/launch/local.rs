// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Child;

use crate::config::web::LaunchRecord;
use crate::error::{Error, Result};

use super::runtime::{self, ProcessIdentity};

struct StartingChild {
    child: Option<Child>,
}

impl Drop for StartingChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}

pub(super) async fn start(root: &Path, launch: &LaunchRecord) -> Result<ProcessIdentity> {
    if !launch.binary_path.is_file() || !launch.service_config_path.is_file() {
        return Err(Error::Config(
            "launch binary and service config must exist".into(),
        ));
    }
    let log_dir = launch.workspace.join("log");
    std::fs::create_dir_all(&log_dir)?;
    let log_path = log_dir.join(format!("{}.launch.log", launch.service_id));
    let output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let child = crate::lifecycle::detached_command(&launch.binary_path)
        .args(launch.command_args())
        .current_dir(&launch.workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::from(output.try_clone()?))
        .stderr(Stdio::from(output))
        .kill_on_drop(false)
        .spawn()?;
    let mut starting = StartingChild { child: Some(child) };
    let child = starting.child.as_mut().expect("starting child exists");
    let pid = child
        .id()
        .ok_or_else(|| Error::Config("launch child has no process identity".into()))?;
    wait_ready(child, launch, &log_path).await?;
    let identity = runtime::local_identity(pid)?
        .ok_or_else(|| Error::Config("launch child exited before identity capture".into()))?;
    runtime::save(root, launch, identity)?;
    let mut child = starting.child.take().expect("starting child exists");
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
    Ok(identity)
}

async fn wait_ready(child: &mut Child, launch: &LaunchRecord, log_path: &Path) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .map_err(|error| Error::Config(error.to_string()))?;
    let started = tokio::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(Error::UpstreamRpc {
                node_id: launch.node_id.to_string(),
                status: format!(
                    "{} exited with {status}; log: {}",
                    launch.service_id,
                    log_path.display()
                ),
            });
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
                reason: format!(
                    "{} did not become ready; log: {}",
                    launch.service_id,
                    log_path.display()
                ),
            });
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
