// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::state::{State, LABEL, ROOT_LABEL};
use crowdb_console_shared::error::{Error, Result};
use serde_json::Value;
use std::time::Duration;

pub(super) async fn docker(args: &[String]) -> Result<String> {
    let program = std::env::var_os("CROWDB_CONTAINER_RUNTIME").unwrap_or_else(|| "docker".into());
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new(program)
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| Error::Config("container runtime command exceeded 60 seconds".into()))??;
    if !output.status.success() {
        return Err(Error::Config(format!(
            "container runtime {}: {}",
            args.first().map_or("operation", String::as_str),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    String::from_utf8(output.stdout).map_err(|e| Error::Config(e.to_string()))
}

pub(super) async fn inspect(kind: &str, name: &str) -> Result<Option<Value>> {
    let args = if kind == "container" {
        vec!["inspect".into(), name.into()]
    } else {
        vec![kind.into(), "inspect".into(), name.into()]
    };
    match docker(&args).await {
        Ok(body) => Ok(serde_json::from_str::<Vec<Value>>(&body)
            .map_err(|e| Error::Config(e.to_string()))?
            .into_iter()
            .next()),
        Err(Error::Config(message))
            if message.contains("No such object")
                || message.contains("No such container")
                || (message.contains("network ") && message.contains(" not found")) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn verify(state: &State, index: usize, value: &Value) -> Result<()> {
    let labels = &value["Config"]["Labels"];
    let expected_name = format!("/{}", state.node_name(index));
    let mounted = value["Mounts"].as_array().is_some_and(|mounts| {
        mounts.iter().any(|mount| {
            mount["Source"].as_str() == state.data(index).to_str()
                && mount["Destination"] == "/opt/crowdb/data"
        })
    });
    let network = &value["NetworkSettings"]["Networks"][&state.network];
    let address = state
        .addresses
        .get(index)
        .ok_or_else(|| Error::Config("cluster has no fixed node address".into()))?;
    let fixed = network["IPAMConfig"]["IPv4Address"] == *address
        && (value["State"]["Running"] != true || network["IPAddress"] == *address);
    if labels[LABEL] != state.id
        || labels[ROOT_LABEL].as_str() != state.root.to_str()
        || value["Image"] != state.image
        || value["Name"] != expected_name
        || !mounted
        || !fixed
    {
        return Err(Error::Config(
            "refusing a container with different cluster, image or data ownership".into(),
        ));
    }
    Ok(())
}

pub(super) fn verify_network(state: &State, value: &Value) -> Result<()> {
    if value["Name"] != state.network
        || value["Labels"][LABEL] != state.id
        || value["Labels"][ROOT_LABEL].as_str() != state.root.to_str()
    {
        return Err(Error::Config(
            "refusing a network with different cluster ownership".into(),
        ));
    }
    Ok(())
}

pub(super) async fn ensure_network(state: &mut State) -> Result<()> {
    if let Some(value) = inspect("network", &state.network).await? {
        verify_network(state, &value)?;
    } else {
        docker(&[
            "network".into(),
            "create".into(),
            "--subnet".into(),
            format!(
                "10.{}.{}.0/24",
                u8::from_str_radix(&state.id[..2], 16).map_err(|e| Error::Config(e.to_string()))?,
                u8::from_str_radix(&state.id[2..4], 16).map_err(|e| Error::Config(e.to_string()))?
            ),
            "--label".into(),
            format!("{LABEL}={}", state.id),
            "--label".into(),
            format!("{ROOT_LABEL}={}", state.root.display()),
            state.network.clone(),
        ])
        .await?;
    }
    let network = inspect("network", &state.network)
        .await?
        .ok_or_else(|| Error::Config("allocated cluster network is absent".into()))?;
    verify_network(state, &network)?;
    let subnet = network["IPAM"]["Config"][0]["Subnet"]
        .as_str()
        .and_then(|s| s.split_once('/'))
        .ok_or_else(|| Error::Config("Docker bridge needs a reachable IPv4 subnet".into()))?;
    let address = subnet
        .0
        .parse::<std::net::Ipv4Addr>()
        .map_err(|e| Error::Config(e.to_string()))?;
    let prefix = subnet
        .1
        .parse::<u32>()
        .map_err(|e| Error::Config(e.to_string()))?;
    if prefix == 0 || prefix > 29 || u32::from(address) & (!u32::MAX >> prefix) != 0 {
        return Err(Error::Config(
            "cluster bridge needs an aligned IPv4 subnet with three usable node addresses".into(),
        ));
    }
    let base = u32::from(address);
    let addresses: Vec<_> = (2..5)
        .map(|n| std::net::Ipv4Addr::from(base + n).to_string())
        .collect();
    if !state.addresses.is_empty() && state.addresses != addresses {
        return Err(Error::Config(
            "cluster network changed fixed node addresses".into(),
        ));
    }
    state.addresses = addresses;
    state.save()
}

pub(super) async fn ensure_nodes(state: &State) -> Result<()> {
    let physical = docker(&["info".into(), "--format".into(), "{{.ID}}".into()]).await?;
    for index in 0..3 {
        let name = state.node_name(index);
        if let Some(value) = inspect("container", &name).await? {
            verify(state, index, &value)?;
            if value["State"]["Running"] != true {
                docker(&["start".into(), name]).await?;
            }
            continue;
        }
        let data = state.data(index);
        std::fs::create_dir_all(&data)?;
        let disk = data.join("disk.img");
        if !disk.exists() {
            std::fs::File::create(&disk)?.set_len(16 * 1024 * 1024 * 1024)?;
        }
        super::maintenance::run(
            state,
            &format!("prepare-{index}"),
            &data,
            "/bin/chown",
            &["-R", "crowdb:crowdb", "/cluster/data"],
        )
        .await?;
        let mut args = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            name,
            "--network".into(),
            state.network.clone(),
            "--ip".into(),
            state.addresses[index].clone(),
            "--label".into(),
            format!("{LABEL}={}", state.id),
            "--label".into(),
            format!("{ROOT_LABEL}={}", state.root.display()),
            "--cpus".into(),
            state.node_cpus.to_string(),
            "--memory".into(),
            format!("{}m", state.memory_mib),
            "--pids-limit".into(),
            "512".into(),
            "--ulimit".into(),
            "core=-1".into(),
            "--mount".into(),
            format!("type=bind,source={},target=/opt/crowdb/data", data.display()),
            "--env".into(),
            "CROWDB_STARTUP_MODE=manual".into(),
            "--env".into(),
            format!("CROWDB_PHYSICAL_HOST_ID=docker-{}", physical.trim()),
        ];
        if std::env::var_os("CROWDB_SSH_PASSWORD").is_some() {
            args.extend(["--env".into(), "CROWDB_SSH_PASSWORD".into()]);
        }
        args.push(state.image.clone());
        docker(&args).await?;
    }
    Ok(())
}
