// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::lifecycle::{DeployDiskdbBody, DiskdbDeployResult};
use crate::{
    error::{err_404, err_409, err_502, map_config_err},
    state::AppState,
};
use axum::{http::StatusCode, Json};
use crowdb_console_shared::deployment::services;
use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};

pub(super) async fn deploy(
    state: &AppState,
    node_id: u64,
    body: &DeployDiskdbBody,
) -> Result<(StatusCode, Json<DiskdbDeployResult>), crate::services::Failure> {
    let (listen, http, rpc) = super::lifecycle::validate_diskdb_ports(body)?;
    let binding = crate::node::cluster_binding(state)?.ok_or_else(|| err_409("Node is unbound"))?;
    let mut node = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .node(node_id)
        .cloned()
        .ok_or_else(|| err_404("Node not found"))?;
    node.ssh_key = Some(crate::node::node_key_path(state).to_string_lossy().into_owned());
    let configuration = format!("[server]\nrpc_workers = 2\nlisten_addr = {:?}\nhttp_listen_addr = {:?}\nrpc_listen_addr = {:?}\ninstance_id = {:?}\nkv_server_mgmt_seeds = {:?}\n",
        format!("{}:{listen}", node.host), format!("{}:{http}", node.host), format!("{}:{rpc}", node.host), node_id.to_string(), binding.management_seeds);
    let intent = NodeServiceIntent {
        cluster_id: binding.bootstrap.cluster_id,
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id,
        service_id: format!("diskdb-{node_id}"),
        kind: "diskdb".into(),
        action: NodeServiceAction::Start,
        configuration,
        environment: std::collections::BTreeMap::default(),
    };
    let reply = services::execute(state.kv_client().await.as_ref(), &node, intent)
        .await
        .map_err(map_config_err)?;
    crate::services::remote::refresh(state)
        .await
        .map_err(map_config_err)?;
    state.persist().map_err(|error| err_502(error.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(DiskdbDeployResult {
            node_id,
            endpoint: format!("http://{}:{rpc}", node.host),
            pid: reply["pid"]
                .as_u64()
                .and_then(|pid| u32::try_from(pid).ok())
                .ok_or_else(|| err_502("Monitor returned no service PID"))?,
        }),
    ))
}

pub(super) async fn restart(
    state: &AppState,
    node_id: u64,
) -> Result<Json<DiskdbDeployResult>, crate::services::Failure> {
    let reply = crate::services::remote::act(state, &format!("diskdb-{node_id}"), NodeServiceAction::Restart)
        .await?
        .0;
    let endpoint = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .servers
        .iter()
        .find(|entry| entry.id == format!("diskdb-{node_id}"))
        .map(|entry| entry.url.clone())
        .ok_or_else(|| err_404("Service not found"))?;
    Ok(Json(DiskdbDeployResult {
        node_id,
        endpoint,
        pid: reply["pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or_else(|| err_502("Monitor returned no PID"))?,
    }))
}
