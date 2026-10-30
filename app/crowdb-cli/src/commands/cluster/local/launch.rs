// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One/three normal OCI nodes; the monitor owns bootstrap and preparation.

use super::{
    docker::{labels, object_name, require_owned, strings, Docker},
    state::{self, invalid, Launch},
    StartArgs,
};
use crowdb_console_shared::error::Result;
use serde_json::{json, Value};
use std::time::Duration;

pub(super) async fn accepted(docker: &Docker, cluster: &str, args: &StartArgs) -> Result<Launch> {
    if let Some(previous) = Launch::load(cluster, &state::directory())? {
        docker.check(&previous).await?;
        if args.nodes.is_some_and(|value| value != previous.nodes)
            || args.port_base.is_some_and(|value| value != previous.port_base)
            || args.image.as_ref().is_some_and(|value| value != &previous.image)
            || args
                .cpus
                .is_some_and(|value| value.to_bits() != previous.cpus.to_bits())
            || args.memory_mib.is_some_and(|value| value != previous.memory_mib)
        {
            return Err(invalid("launch arguments differ from accepted cluster inputs"));
        }
        return Ok(previous);
    }
    let image = args.image.as_deref().unwrap_or("crowdb-node:dev");
    let metadata = docker.inspect("image", image).await.map_err(|error| {
        invalid(&format!(
            "image inspection failed: {error}; build and load the OCI node image before start"
        ))
    })?;
    if metadata["Os"] != "linux"
        || metadata["Architecture"] != "amd64"
        || metadata["Config"]["Labels"]["org.opencontainers.image.source"]
            != "https://github.com/buzzcrow/crowdb"
    {
        return Err(invalid("local start requires the Linux amd64 CROWDB node image"));
    }
    let record = Launch {
        version: 2,
        cluster: cluster.into(),
        owner: uuid::Uuid::new_v4().to_string(),
        runtime: docker.identity().await?,
        nodes: args.nodes.unwrap_or(1),
        port_base: args.port_base.unwrap_or(9090),
        image: image.into(),
        image_id: metadata["Id"]
            .as_str()
            .ok_or_else(|| invalid("image has no immutable ID"))?
            .into(),
        cpus: args.cpus.unwrap_or(1.0),
        memory_mib: args.memory_mib.unwrap_or(1536),
    };
    record.seal(&state::directory())?;
    Ok(record)
}

pub(super) async fn run(docker: &Docker, record: &Launch) -> Result<()> {
    docker.check(record).await?;
    docker.inspect("image", &record.image_id).await?;
    let network = record.resource_name("network", 0);
    let bridge = docker.ensure(record, "network", &network).await?;
    if bridge["Driver"] != "bridge" {
        return Err(invalid("local cluster requires its owned bridge"));
    }
    let containers = docker.objects("container").await?;
    for index in 1..=record.nodes {
        let name = record.resource_name("container", index);
        let volume = record.resource_name("volume", index);
        docker.ensure(record, "volume", &volume).await?;
        if let Some(previous) = containers.iter().find(|item| object_name(item) == name) {
            verify(previous, record, index)?;
            if previous["State"]["Running"] != true {
                docker.check(record).await?;
                docker
                    .call(&strings(&[
                        "start",
                        previous["Id"]
                            .as_str()
                            .ok_or_else(|| invalid("container ID absent"))?,
                    ]))
                    .await?;
            }
        } else {
            docker.check(record).await?;
            let identity = docker.call(&arguments(record, index)).await?;
            verify(&docker.inspect("container", &identity).await?, record, index)?;
        }
    }
    Ok(())
}

fn arguments(record: &Launch, index: u8) -> Vec<String> {
    let name = record.resource_name("container", index);
    let network = record.resource_name("network", 0);
    let volume = record.resource_name("volume", index);
    let mut args = strings(&["run", "-d", "--pull", "never", "--name", &name]);
    args.extend(labels(record));
    args.extend(strings(&[
        "--network",
        &network,
        "--cpus",
        &record.cpus.to_string(),
        "--memory",
        &format!("{}m", record.memory_mib),
        "--mount",
        &format!("type=volume,source={volume},target=/opt/crowdb/data"),
    ]));
    let base = record.port_base + u16::from(index - 1) * 10;
    for ((offset, protocol), internal) in [(0_u16, ""), (1, "S3"), (2, "ICEBERG"), (3, "DATASET")]
        .into_iter()
        .zip(record.container_ports())
    {
        args.extend(strings(&[
            "-p",
            &format!("127.0.0.1:{}:{internal}", base + offset),
        ]));
        if !protocol.is_empty() {
            args.extend(strings(&[
                "-e",
                &format!("CROWDB_{protocol}_PUBLIC_URI=http://127.0.0.1:{}", base + offset),
            ]));
        }
    }
    args.extend(strings(&[
        "-e",
        &format!("CROWDB_CLUSTER={}", record.cluster),
        "-e",
        &format!("CROWDB_INITIAL_VOTERS={}", record.nodes),
    ]));
    args.push(record.image_id.clone());
    args
}

pub(super) fn verify(item: &Value, record: &Launch, index: u8) -> Result<()> {
    require_owned(item, record, "container")?;
    let base = record.port_base + u16::from(index - 1) * 10;
    let mut ports = serde_json::Map::new();
    for (offset, internal) in (0_u16..).zip(record.container_ports()) {
        ports.insert(
            format!("{internal}/tcp"),
            json!([{"HostIp":"127.0.0.1", "HostPort":(base+offset).to_string()}]),
        );
    }
    let mounts = item["Mounts"]
        .as_array()
        .ok_or_else(|| invalid("container mounts absent"))?;
    let mounted: Vec<_> = mounts
        .iter()
        .filter(|mount| mount["Destination"] == "/opt/crowdb/data")
        .collect();
    let nano_cpus = u64::try_from(Duration::from_secs_f64(record.cpus).as_nanos())
        .map_err(|_| invalid("invalid CPU boundary"))?;
    if item["Image"] != record.image_id
        || item["HostConfig"]["NanoCpus"] != nano_cpus
        || item["HostConfig"]["Memory"] != record.memory_mib * 1024 * 1024
        || item["HostConfig"]["NetworkMode"] != record.resource_name("network", 0)
        || item["HostConfig"]["PortBindings"] != Value::Object(ports)
        || mounted.len() != 1
        || mounted[0]["Type"] != "volume"
        || mounted[0]["RW"] != true
        || mounted[0]["Name"] != record.resource_name("volume", index)
    {
        return Err(invalid(
            "container image, resources, endpoints or volume differ from accepted inputs",
        ));
    }
    let environment = item["Config"]["Env"]
        .as_array()
        .ok_or_else(|| invalid("container environment absent"))?;
    let mut expected = vec![
        format!("CROWDB_CLUSTER={}", record.cluster),
        format!("CROWDB_INITIAL_VOTERS={}", record.nodes),
    ];
    for (offset, protocol) in [(1, "S3"), (2, "ICEBERG"), (3, "DATASET")] {
        expected.push(format!(
            "CROWDB_{protocol}_PUBLIC_URI=http://127.0.0.1:{}",
            base + offset
        ));
    }
    if expected
        .iter()
        .any(|expected| !environment.iter().any(|value| value.as_str() == Some(expected)))
    {
        return Err(invalid(
            "container deployment policy differs from accepted inputs",
        ));
    }
    Ok(())
}
