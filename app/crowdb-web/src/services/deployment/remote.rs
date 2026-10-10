// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{Deploy, Kind};
use crate::{
    error::{err_404, err_409, err_502, map_config_err},
    services::Failure,
    state::AppState,
};
use axum::{http::StatusCode, Json};
use crowdb_console_shared::{
    config::{NodeEntry, ServerEntry},
    lifecycle::{self, ChunkdbDeployRequest, DiskioDeployRequest},
};
use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};

pub(super) async fn deploy(
    state: &AppState,
    node_id: u64,
    body: &Deploy,
) -> Result<(StatusCode, Json<Value>), Failure> {
    let binding = crate::node::cluster_binding(state)?.ok_or_else(|| err_409("Cluster binding required"))?;
    let mut node = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .node(node_id)
        .cloned()
        .ok_or_else(|| err_404("Node not found"))?;
    node.ssh_key = Some(crate::node::node_key_path(state).to_string_lossy().into_owned());
    let id = format!("{}-{}", body.kind.name(), body.instance_id);
    let _operation =
        crate::services::Operation::claim(state, vec![format!("node/{node_id}"), format!("service/{id}")])?;
    if matches!(body.kind, Kind::Chunkdb | Kind::ChunkKv) && !body.test_single_node {
        super::geometry::validate(state).await?;
        if let Some(reason) = crate::services::dependencies::storage_wait_reason(state).await {
            return Err(err_409(reason));
        }
    }
    if matches!(body.kind, Kind::AccessServer) {
        prepare_credentials(state, &binding).await?;
    }
    let configuration = configuration(state, &node, &id, &binding.management_seeds, body).await?;
    let intent = NodeServiceIntent {
        cluster_id: binding.bootstrap.cluster_id,
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id,
        service_id: id.clone(),
        kind: if matches!(body.kind, Kind::AccessServer) {
            "access".into()
        } else {
            body.kind.name().into()
        },
        action: NodeServiceAction::Start,
        configuration,
        environment: environment(body, &node),
    };
    let result =
        crowdb_console_shared::deployment::services::execute(state.kv_client().await.as_ref(), &node, intent)
            .await
            .map_err(map_config_err)?;
    let host = if node.host.contains(':') {
        format!("[{}]", node.host)
    } else {
        node.host.clone()
    };
    let url = match body.kind {
        Kind::Diskio => format!("{host}:{}", body.rpc_port.unwrap()),
        _ => format!("http://{host}:{}", body.http_port.unwrap()),
    };
    let mut entry = ServerEntry::new(&id, url);
    entry.service_type = body.kind.service_type();
    entry.node_id = Some(node_id);
    entry.rpc_url = body.rpc_port.map(|port| format!("{host}:{port}"));
    entry.rest_port = body.http_port;
    entry.rpc_port = body.rpc_port;
    entry.auto_start = true;
    {
        let mut config = state.config.write().map_err(|error| err_502(error.to_string()))?;
        config.servers.retain(|server| server.id != id);
        config.servers.push(entry);
    }
    state.persist().map_err(|error| err_502(error.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"id": id, "node_id": node_id, "pid": result["pid"], "service_type": body.kind, "operation_id": result["operation_id"]}),
        ),
    ))
}

async fn prepare_credentials(
    state: &AppState,
    binding: &crowdb_protocol::mgmt::node::NodeBinding,
) -> Result<(), Failure> {
    let value = crowdb_console_shared::deployment::admission::local_control(
        &crate::node::control_socket(),
        &crowdb_protocol::mgmt::node::NodeControl::ServiceCredentials {
            cluster_id: binding.bootstrap.cluster_id.clone(),
        },
    )
    .await
    .map_err(map_config_err)?;
    let environment = value["environment"]
        .as_str()
        .ok_or_else(|| err_502("Cluster service credentials unavailable"))?;
    crowdb_monitor::ServerCredentials::import(state.runtime_root.as_ref(), environment)
        .map_err(|error| err_502(error.to_string()))?;
    let spec = crowdb_console_shared::config::LocalLaunchSpec {
        program: "/opt/crowdb/bin/crowdb-access-server".into(),
        args: Vec::new(),
        workdir: state.runtime_root.to_string_lossy().into_owned(),
        env: BTreeMap::new(),
        env_file: None,
        readiness_url: None,
    };
    super::credentials::prepare(state.runtime_root.as_ref(), &spec, &binding.management_seeds).await
}

async fn configuration(
    state: &AppState,
    node: &NodeEntry,
    id: &str,
    seeds: &[String],
    body: &Deploy,
) -> Result<String, Failure> {
    match body.kind {
        Kind::Chunkdb => {
            super::chunk_slots::prepare(state, body.instance_id, body.dynamic_ownership).await?;
            Ok(lifecycle::chunkdb_config(
                &ChunkdbDeployRequest {
                    server_id: id.into(),
                    instance_id: body.instance_id,
                    http_port: body.http_port.unwrap(),
                    rpc_port: body.rpc_port.unwrap(),
                    kv_server_mgmt_seeds: seeds.to_vec(),
                    allow_unsafe_ec: body.test_single_node,
                    dynamic_ownership: body.dynamic_ownership,
                    rpc_workers: None,
                    kv_connections: None,
                    kv_client_rpc_workers: None,
                    diskdb_connections: None,
                    diskdb_client_rpc_workers: None,
                    metrics_interval: None,
                },
                node,
            ))
        }
        Kind::Diskio => Ok(lifecycle::diskio_config(
            &DiskioDeployRequest {
                server_id: id.into(),
                instance_id: body.instance_id,
                rpc_port: body.rpc_port.unwrap(),
                node_id: node.id,
                rack_id: node.rack_id,
                disk_group_id: body.disk_group_id.unwrap_or(0),
                kv_server_mgmt_seeds: seeds.to_vec(),
                dummy_disk_type: "mem".into(),
                rpc_workers: None,
                metrics_interval: None,
                o_direct: true,
                disks: Vec::new(),
            },
            node,
            Path::new("/opt/crowdb/data/log/diskio"),
        )),
        Kind::ChunkKv => encode(super::launch::chunk_kv_config(body, seeds, node).await?),
        Kind::AccessServer => encode(super::launch::access_config(body, seeds, node)),
    }
}

fn encode(value: Value) -> Result<String, Failure> {
    let value = toml::Value::try_from(value).map_err(|error| err_502(error.to_string()))?;
    toml::to_string_pretty(&value).map_err(|error| err_502(error.to_string()))
}

fn environment(body: &Deploy, node: &NodeEntry) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    if matches!(body.kind, Kind::Chunkdb) {
        env.insert(
            "CROWDB_CHUNKDB_OWNERSHIP_POLICY".into(),
            if body.dynamic_ownership {
                "dynamic"
            } else {
                "fixed"
            }
            .into(),
        );
    }
    if matches!(body.kind, Kind::AccessServer) {
        env.insert(
            "CROWDB_ICEBERG_PUBLIC_URI".into(),
            format!("http://{}:{}", node.host, body.http_port.unwrap()),
        );
        env.insert(
            "CROWDB_S3_PUBLIC_URI".into(),
            format!("http://{}:{}", node.host, body.s3_port.unwrap()),
        );
        env.insert(
            "CROWDB_ACCESS_HEALTH_LISTEN".into(),
            format!("{}:{}", node.host, body.health_port.unwrap()),
        );
    }
    env
}
