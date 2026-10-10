// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared service projections and target-monitor lifecycle operations.

use super::Failure;
use crate::{
    error::{err_404, err_409, err_502, map_config_err},
    state::AppState,
};
use axum::Json;
use crowdb_console_shared::{
    config::{ServerEntry, ServiceType},
    deployment::services,
};
use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};
use serde_json::{json, Value};

pub(crate) async fn act(
    state: &AppState,
    id: &str,
    action: NodeServiceAction,
) -> Result<Json<Value>, Failure> {
    let binding = crate::node::cluster_binding(state)?.ok_or_else(|| err_409("Node is unbound"))?;
    let mut intent = services::list(state.kv_client().await.as_ref())
        .await
        .map_err(map_config_err)?
        .into_iter()
        .find(|intent| intent.service_id == id && intent.cluster_id == binding.bootstrap.cluster_id)
        .ok_or_else(|| err_404("Service intent not found"))?;
    let _operation = super::Operation::claim(
        state,
        vec![format!("node/{}", intent.node_id), format!("service/{id}")],
    )?;
    let mut node = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .node(intent.node_id)
        .cloned()
        .ok_or_else(|| err_404("Node not found"))?;
    node.ssh_key = Some(crate::node::node_key_path(state).to_string_lossy().into_owned());
    intent.operation_id = uuid::Uuid::new_v4().to_string();
    intent.action = action;
    let result = services::execute(state.kv_client().await.as_ref(), &node, intent)
        .await
        .map_err(map_config_err)?;
    refresh(state).await.map_err(map_config_err)?;
    Ok(Json(
        json!({"id": id, "pid":result["pid"], "removed":matches!(action, NodeServiceAction::Delete), "data_preserved":true}),
    ))
}

pub(crate) async fn refresh(state: &AppState) -> crowdb_console_shared::error::Result<()> {
    let intents = services::list(state.kv_client().await.as_ref()).await?;
    let mut config = state
        .config
        .write()
        .map_err(|error| crowdb_console_shared::error::Error::Config(error.to_string()))?;
    for intent in intents {
        config.servers.retain(|entry| entry.id != intent.service_id);
        if matches!(intent.action, NodeServiceAction::Delete) {
            continue;
        }
        if let Some(entry) = project(&intent) {
            config.servers.push(entry);
        }
    }
    Ok(())
}

fn project(intent: &NodeServiceIntent) -> Option<ServerEntry> {
    let value: toml::Value = toml::from_str(&intent.configuration).ok()?;
    let (kind, http, rpc) = match intent.kind.as_str() {
        "diskdb" => (
            ServiceType::Diskdb,
            &["server", "rpc_listen_addr"][..],
            &["server", "rpc_listen_addr"][..],
        ),
        "chunkdb" => (
            ServiceType::Chunkdb,
            &["server", "http_listen_addr"][..],
            &["server", "rpc_listen_addr"][..],
        ),
        "diskio" => (
            ServiceType::Diskio,
            &["server", "rpc_listen_addr"][..],
            &["server", "rpc_listen_addr"][..],
        ),
        "chunk-kv" => (
            ServiceType::ChunkKv,
            &["http_listen_addr"][..],
            &["rpc_listen_addr"][..],
        ),
        "access" => (ServiceType::AccessServer, &["iceberg", "listen"][..], &[][..]),
        _ => return None,
    };
    let diskio_address = (intent.kind == "diskio")
        .then(|| {
            Some(format!(
                "{}:{}",
                value.get("server")?.get("bind_address")?.as_str()?,
                value.get("server")?.get("listen_port")?.as_integer()?
            ))
        })
        .flatten();
    let address = diskio_address.as_deref().or_else(|| lookup(&value, http))?;
    let mut entry = ServerEntry::new(
        &intent.service_id,
        if intent.kind == "diskio" {
            address.to_owned()
        } else {
            format!("http://{address}")
        },
    );
    entry.node_id = Some(intent.node_id);
    entry.service_type = kind;
    entry.rpc_url = diskio_address
        .clone()
        .or_else(|| lookup(&value, rpc).map(str::to_owned));
    entry.rest_port = address.rsplit_once(':').and_then(|(_, port)| port.parse().ok());
    entry.rpc_port = entry
        .rpc_url
        .as_ref()
        .and_then(|url| url.rsplit_once(':'))
        .and_then(|(_, port)| port.parse().ok());
    entry.auto_start = matches!(
        intent.action,
        NodeServiceAction::Start | NodeServiceAction::Restart
    );
    Some(entry)
}

fn lookup<'a>(value: &'a toml::Value, path: &[&str]) -> Option<&'a str> {
    if path.is_empty() {
        return None;
    }
    let mut value = value;
    for key in path {
        value = value.get(*key)?;
    }
    value.as_str()
}

pub(crate) async fn kv_action(
    state: &AppState,
    node_id: u64,
    action: NodeServiceAction,
) -> Result<Value, Failure> {
    let binding = crate::node::cluster_binding(state)?.ok_or_else(|| err_409("Node is unbound"))?;
    let mut node = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .node(node_id)
        .cloned()
        .ok_or_else(|| err_404("Node not found"))?;
    node.ssh_key = Some(crate::node::node_key_path(state).to_string_lossy().into_owned());
    let intent = NodeServiceIntent {
        cluster_id: binding.bootstrap.cluster_id,
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id,
        service_id: format!("kv-{node_id}"),
        kind: "kv".into(),
        action,
        configuration: format!("node_id = {node_id}\n"),
        environment: std::collections::BTreeMap::new(),
    };
    services::execute(state.kv_client().await.as_ref(), &node, intent)
        .await
        .map_err(map_config_err)
}
