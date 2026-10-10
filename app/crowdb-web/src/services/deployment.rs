// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crowdb_console_shared::config::{NodeEntry, ServerEntry, ServiceType};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{operation::Operation, Failure};
use crate::{
    error::{err_400, err_404, err_409, err_500},
    state::AppState,
};

mod chunk_slots;
mod credentials;
mod geometry;
mod launch;
mod remote;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Kind {
    Chunkdb,
    Diskio,
    ChunkKv,
    AccessServer,
}

impl Kind {
    fn service_type(self) -> ServiceType {
        match self {
            Self::Chunkdb => ServiceType::Chunkdb,
            Self::Diskio => ServiceType::Diskio,
            Self::ChunkKv => ServiceType::ChunkKv,
            Self::AccessServer => ServiceType::AccessServer,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Chunkdb => "chunkdb",
            Self::Diskio => "diskio",
            Self::ChunkKv => "chunk-kv",
            Self::AccessServer => "access-server",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Deploy {
    #[serde(default)]
    dynamic_ownership: bool,
    kind: Kind,
    #[serde(deserialize_with = "deserialize_id")]
    instance_id: u64,
    #[serde(default)]
    http_port: Option<u16>,
    #[serde(default)]
    rpc_port: Option<u16>,
    #[serde(default)]
    s3_port: Option<u16>,
    #[serde(default)]
    health_port: Option<u16>,
    #[serde(default)]
    disk_group_id: Option<u64>,
    #[serde(default)]
    metadata_store_id: Option<u64>,
    #[serde(default)]
    bootstrap_group_id: Option<u64>,
    #[serde(default)]
    test_single_node: bool,
}

fn deserialize_id<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(value) => value
            .parse()
            .map_err(|_| serde::de::Error::custom("Invalid instance ID")),
        Value::Number(value) => value
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("Invalid instance ID")),
        _ => Err(serde::de::Error::custom("Instance ID must be a decimal integer")),
    }
}

pub(super) async fn deploy(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<Deploy>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let kind = body.kind.name();
    let listeners = json!({"http_port":body.http_port,"rpc_port":body.rpc_port,"s3_port":body.s3_port,"health_port":body.health_port});
    deploy_inner(state, node_id, body).await.map_err(|(status, Json(error))| {
        (status, Json(json!({"error":error.error,"service":kind,"node_id":node_id,"listeners":listeners,"retryable":true})))
    })
}

async fn deploy_inner(
    state: AppState,
    node_id: u64,
    body: Deploy,
) -> Result<(StatusCode, Json<Value>), Failure> {
    validate(&body)?;
    if state.node_monitor_url.is_some() {
        return remote::deploy(&state, node_id, &body).await;
    }
    if !super::dependencies::group0_ready(&state).await {
        return Err(err_409(
            "Group 0 is not ready; deploy Paxos-KV and initialize Group 0 before starting this service",
        ));
    }
    let id = format!("{}-{}", body.kind.name(), body.instance_id);
    let mut claims = vec![format!("node/{node_id}"), format!("service/{id}")];
    if matches!(body.kind, Kind::AccessServer) {
        claims.push("access-credentials".into());
    }
    let operation = Operation::claim(&state, claims)?;
    let ports: Vec<_> = [body.http_port, body.rpc_port, body.s3_port, body.health_port]
        .into_iter()
        .flatten()
        .collect();
    let port_claim = super::defaults::claim_ports(&state, &ports)?;
    let (node, seeds) = inputs(&state, node_id, &id, &body)?;
    if matches!(body.kind, Kind::Chunkdb | Kind::ChunkKv) && !body.test_single_node {
        if let Some(reason) = super::dependencies::storage_wait_reason(&state).await {
            return Err(err_409(reason));
        }
        geometry::validate(&state).await?;
    }
    // Request cancellation cannot abandon a spawned process before its registration.
    tokio::spawn(async move {
        let _operation = operation;
        let _ports = port_claim;
        run(&state, &node, &id, &seeds, &body).await
    })
    .await
    .map_err(|error| err_500(format!("Deployment task failed: {error}")))?
}

fn validate(body: &Deploy) -> Result<(), Failure> {
    if body.instance_id == 0 || body.instance_id > (u64::MAX >> 1) {
        return Err(err_400("Instance ID must be in 1..=9223372036854775807"));
    }
    let ports = match body.kind {
        Kind::Diskio => vec![body.rpc_port],
        Kind::AccessServer => vec![body.http_port, body.s3_port, body.health_port],
        _ => vec![body.http_port, body.rpc_port],
    };
    if ports.iter().any(|port| port.map_or(true, |port| port == 0))
        || ports.len() != ports.iter().collect::<std::collections::HashSet<_>>().len()
    {
        return Err(err_400("Every listener needs a distinct port in 1..=65535"));
    }
    if matches!(body.kind, Kind::ChunkKv) && body.metadata_store_id.is_none() {
        return Err(err_400("Chunk-KV requires a metadata store"));
    }
    if body.bootstrap_group_id == Some(0) {
        return Err(err_400("Partition journal metadata must use a non-system group"));
    }
    if (!matches!(body.kind, Kind::Diskio) && body.disk_group_id.is_some())
        || (!matches!(body.kind, Kind::ChunkKv)
            && (body.metadata_store_id.is_some() || body.bootstrap_group_id.is_some()))
        || (!matches!(body.kind, Kind::AccessServer) && body.s3_port.is_some())
        || (!matches!(body.kind, Kind::AccessServer) && body.health_port.is_some())
        || (matches!(body.kind, Kind::AccessServer) && body.rpc_port.is_some())
        || (matches!(body.kind, Kind::Diskio) && body.http_port.is_some())
    {
        return Err(err_400(
            "Deployment parameters do not match the selected service type",
        ));
    }
    if [
        body.metadata_store_id,
        body.bootstrap_group_id,
        body.disk_group_id,
    ]
    .into_iter()
    .flatten()
    .any(|id| id > i64::MAX as u64)
    {
        return Err(err_400("Deployment IDs must fit signed 64-bit TOML integers"));
    }
    Ok(())
}

fn inputs(
    state: &AppState,
    node_id: u64,
    id: &str,
    body: &Deploy,
) -> Result<(NodeEntry, Vec<String>), Failure> {
    let config = state.config.read().unwrap();
    if config.servers.iter().any(|entry| entry.id == id) {
        return Err(err_409("Service instance already exists"));
    }
    let mut node = config
        .node(node_id)
        .cloned()
        .ok_or_else(|| err_404("Node not found"))?;
    if !matches!(node.host.as_str(), "127.0.0.1" | "localhost") {
        return Err(err_400(
            "Auxiliary deployment currently supports local nodes only",
        ));
    }
    node.host = "127.0.0.1".into();
    let requested: Vec<_> = [body.http_port, body.rpc_port, body.s3_port, body.health_port]
        .into_iter()
        .flatten()
        .collect();
    for service in &config.servers {
        let public = config
            .local_launches
            .get(&service.id)
            .into_iter()
            .flat_map(|spec| {
                ["CROWDB_S3_PUBLIC_URI", "CROWDB_ICEBERG_PUBLIC_URI"]
                    .into_iter()
                    .filter_map(|name| spec.env.get(name).map(String::as_str))
            });
        let origins = std::iter::once(service.url.as_str())
            .chain(service.rpc_url.as_deref())
            .chain(public);
        for origin in origins {
            let normalized = if origin.contains("://") {
                origin.to_owned()
            } else {
                format!("http://{origin}")
            };
            if let Ok(url) = reqwest::Url::parse(&normalized) {
                if matches!(url.host_str(), Some("localhost" | "127.0.0.1"))
                    && url
                        .port_or_known_default()
                        .is_some_and(|port| requested.contains(&port))
                {
                    return Err(err_409(
                        "A requested listener port belongs to another configured service",
                    ));
                }
            }
        }
    }
    let seeds: Vec<_> = config
        .servers
        .iter()
        .filter(|entry| entry.service_type == ServiceType::PaxosKv)
        .map(|entry| entry.url.clone())
        .collect();
    if seeds.is_empty() {
        return Err(err_409("Deploy and initialize this cluster's KV services first"));
    }
    if let Some(group) = body.disk_group_id.filter(|group| *group != 0) {
        if !config
            .disk_groups
            .iter()
            .any(|entry| entry.id == group && entry.node_id == node_id)
        {
            return Err(err_400("Disk group does not belong to the selected node"));
        }
        let disks: Vec<_> = config
            .disks
            .iter()
            .filter(|disk| disk.node_id == node_id && disk.disk_group_id == group)
            .collect();
        if !body.test_single_node
            && (disks.is_empty() || disks.iter().any(|disk| disk.device_path.is_empty()))
        {
            return Err(err_400(
                "Production DiskIO requires configured disks with device paths; configure them in Capacity",
            ));
        }
    }
    Ok((node, seeds))
}

async fn run(
    state: &AppState,
    node: &NodeEntry,
    id: &str,
    seeds: &[String],
    body: &Deploy,
) -> Result<(StatusCode, Json<Value>), Failure> {
    let root = state
        .prepare_node_workspace(node.id)
        .map_err(|error| err_500(error.to_string()))?;
    // Failed attempts retain their logs/data; a retry gets a fresh workspace.
    let workspace = root
        .join("services")
        .join(id)
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(workspace.parent().unwrap()).map_err(|error| err_500(error.to_string()))?;
    std::fs::create_dir(&workspace).map_err(|error| {
        err_409(format!(
            "Service workspace already exists or cannot be created: {error}"
        ))
    })?;
    let deployed = launch::launch(state, node, id, seeds, body, &workspace).await?;
    let mut entry = ServerEntry::new(id, deployed.url);
    entry.service_type = body.kind.service_type();
    entry.node_id = Some(node.id);
    entry.rpc_url = deployed.rpc_url;
    entry.rpc_port = body.rpc_port;
    entry.rest_port = body.http_port;
    entry.auto_start = true;
    entry.pid = Some(deployed.pid);
    let registration = {
        let mut config = state.config.write().unwrap();
        let result = config.add_server(entry);
        if result.is_ok() {
            config.local_launches.insert(id.into(), deployed.spec);
        }
        result
    };
    if let Err(error) = registration {
        let pid = deployed.pid;
        tokio::task::spawn_blocking(move || crowdb_console_shared::lifecycle::stop_pid(pid))
            .await
            .map_err(|error| err_500(error.to_string()))?
            .map_err(|error| err_500(format!("Registration failed and child cleanup failed: {error}")))?;
        return Err(err_500(format!(
            "Service registration failed; child stopped: {error}"
        )));
    }
    super::publication::publish(state, id, deployed.pid).await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"id":id,"node_id":node.id,"pid":deployed.pid,"service_type":body.kind})),
    ))
}
