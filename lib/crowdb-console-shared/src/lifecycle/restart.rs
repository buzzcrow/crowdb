// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{detached_command, process_is_alive, stop_pid_with_timeout, wait_for_service_ready};
use crate::{
    config::LocalLaunchSpec,
    error::{Error, Result},
};
use std::{path::Path, process::Stdio, time::Duration};

/// Stop and relaunch a locally deployed auxiliary service from its retained
/// launch specification.
///
/// # Errors
/// Returns an error when the old process cannot stop, the replacement cannot
/// start, or its configured readiness endpoint does not become healthy.
pub async fn restart_local_service(server_id: &str, pid: u32, spec: &LocalLaunchSpec) -> Result<u32> {
    let private_env = private_environment(spec)?;
    if pid > 0 && process_is_alive(pid) {
        stop_pid_with_timeout(pid, Duration::from_secs(15))?;
    }
    let workdir = Path::new(&spec.workdir);
    let log_dir = workdir.join("log");
    std::fs::create_dir_all(&log_dir)?;
    let output_path = log_dir.join(format!("{server_id}.restart.stdout.log"));
    let output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output_path)?;
    let mut command = detached_command(&spec.program);
    command
        .args(&spec.args)
        .envs(&spec.env)
        .envs(private_env)
        .current_dir(workdir)
        .stdout(Stdio::from(output.try_clone()?))
        .stderr(Stdio::from(output))
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let new_pid = child.id().ok_or_else(|| Error::Validation {
        field: "pid".into(),
        message: format!("restarted {server_id} child has no pid"),
    })?;
    if let Some(url) = &spec.readiness_url {
        wait_for_service_ready(
            &mut child,
            url,
            &output_path,
            new_pid,
            Duration::from_secs(60),
            server_id,
        )
        .await?;
    } else {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Some(status) = child.try_wait()? {
            return Err(Error::UpstreamRpc {
                node_id: server_id.into(),
                status: format!("restarted process exited early with {status}"),
            });
        }
    }
    std::mem::forget(child);
    Ok(new_pid)
}

fn private_environment(spec: &LocalLaunchSpec) -> Result<std::collections::BTreeMap<String, String>> {
    use std::os::unix::fs::PermissionsExt;
    let Some(path) = &spec.env_file else {
        return Ok(std::collections::BTreeMap::new());
    };
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o077 != 0 || metadata.len() > 8192
    {
        return Err(Error::Config(
            "Service environment must be a private regular file of at most 8 KiB".into(),
        ));
    }
    let body = std::fs::read_to_string(path)?;
    let mut values = std::collections::BTreeMap::new();
    for line in body.lines().filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once('=')
            .ok_or_else(|| Error::Config("Invalid service environment record".into()))?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte == b'_' || byte.is_ascii_digit())
            || value.contains('\0')
            || values.insert(name.to_owned(), value.to_owned()).is_some()
        {
            return Err(Error::Config(
                "Invalid or duplicate service environment name".into(),
            ));
        }
    }
    Ok(values)
}
