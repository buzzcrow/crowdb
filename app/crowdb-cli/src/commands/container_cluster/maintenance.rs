// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    runtime,
    state::{State, LABEL, ROOT_LABEL},
};
use crowdb_console_shared::error::{Error, Result};
use std::path::Path;

/// Bounded release-image filesystem work, recoverable after client interruption.
pub(super) async fn run(
    state: &State,
    operation: &str,
    data: &Path,
    executable: &str,
    arguments: &[&str],
) -> Result<()> {
    let name = format!("{}-{operation}", state.name);
    remove_owned(state, &name, data).await?;
    let mut args = vec![
        "run".into(),
        "--rm".into(),
        "--name".into(),
        name.clone(),
        "--network".into(),
        "none".into(),
        "--user".into(),
        "0".into(),
        "--label".into(),
        format!("{LABEL}={}", state.id),
        "--label".into(),
        format!("{ROOT_LABEL}={}", state.root.display()),
        "--cpu-shares".into(),
        format!("{:.0}", state.node_cpus * 1024.0),
        "--memory".into(),
        "64m".into(),
        "--pids-limit".into(),
        "32".into(),
        "--entrypoint".into(),
        executable.into(),
        "--mount".into(),
        format!("type=bind,source={},target=/cluster/data", data.display()),
        state.image.clone(),
    ];
    args.extend(arguments.iter().map(|argument| (*argument).into()));
    let result = runtime::docker(&args).await;
    let cleanup = remove_owned(state, &name, data).await;
    result?;
    cleanup
}

async fn remove_owned(state: &State, name: &str, data: &Path) -> Result<()> {
    if let Some(value) = runtime::inspect("container", name).await? {
        let mounted = value["Mounts"].as_array().is_some_and(|mounts| {
            mounts.iter().any(|mount| {
                mount["Source"].as_str() == data.to_str() && mount["Destination"] == "/cluster/data"
            })
        });
        if value["Config"]["Labels"][LABEL] != state.id
            || value["Config"]["Labels"][ROOT_LABEL].as_str() != state.root.to_str()
            || value["Image"] != state.image
            || !mounted
        {
            return Err(Error::Config(
                "refusing an unowned filesystem helper container".into(),
            ));
        }
        runtime::docker(&["rm".into(), "-f".into(), name.into()]).await?;
    }
    Ok(())
}
