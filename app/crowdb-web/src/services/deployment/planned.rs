// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Prepare a fixed monitor-owned operation through the existing typed builders.

use crate::{
    error::{err_400, err_409, err_502, map_config_err},
    services::Failure,
    state::AppState,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crowdb_console_shared::{
    config::NodeEntry,
    deployment::{admission::local_control, services::publish_planned_start},
};
use crowdb_protocol::mgmt::node::{
    setup::{NodeSetupRequest, NodeSetupStatus, SetupService, SetupServiceKind},
    NodeBinding, NodeControl, NodeServiceAction, NodeServiceIntent,
};
use serde::Deserialize;
use serde_json::{json, Value};

mod catalog;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Prepare {
    operation_id: String,
    kind: SetupServiceKind,
}

pub(crate) async fn prepare(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<Prepare>,
) -> Result<(StatusCode, Json<Value>), Failure> {
    let binding = crate::node::cluster_binding(&state)?
        .filter(|binding| binding.node_id == node_id)
        .ok_or_else(|| err_409("Only the owning node backend can prepare its setup"))?;
    let socket = crate::node::control_socket();
    let request: NodeSetupRequest = serde_json::from_value(
        local_control(&socket, &NodeControl::NodeSetupIntent)
            .await
            .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    let status: NodeSetupStatus = serde_json::from_value(
        local_control(&socket, &NodeControl::NodeSetup)
            .await
            .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    if request.operation_id != body.operation_id
        || status.operation_id != body.operation_id
        || status.cluster_id.as_ref() != Some(&binding.bootstrap.cluster_id)
        || status.node_id != Some(node_id)
    {
        return Err(err_409("Setup operation does not match accepted local authority"));
    }
    let selected = request
        .choices
        .services
        .iter()
        .find(|service| service.kind == body.kind && service.enabled)
        .ok_or_else(|| err_409("Service is not enabled by accepted setup"))?;
    let operation = status
        .services
        .iter()
        .find(|service| service.kind == body.kind)
        .and_then(|step| step.operation_id.as_deref())
        .ok_or_else(|| err_409("Monitor has not assigned this service operation"))?;
    let _claim = crate::services::Operation::claim(&state, vec![format!("node/{node_id}")])?;
    let directory = state
        .prepare_node_workspace(node_id)
        .map_err(|error| err_502(error.to_string()))?;
    let path = directory.join(format!(
        "setup-service-{}.json",
        uuid::Uuid::parse_str(operation).map_err(|_| err_400("Invalid setup service operation"))?
    ));
    let intent: NodeServiceIntent = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.len() > 65536 {
                return Err(err_409("Invalid prepared service record"));
            }
            serde_json::from_slice(&std::fs::read(&path).map_err(|error| err_502(error.to_string()))?)
                .map_err(|error| err_502(error.to_string()))?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let intent = build(&state, &binding, selected, operation).await?;
            crate::node::save(&path, &intent)?;
            intent
        }
        Err(error) => return Err(err_502(error.to_string())),
    };
    if intent.operation_id != operation
        || intent.node_id != node_id
        || intent.cluster_id != binding.bootstrap.cluster_id
        || intent.action != NodeServiceAction::Start
        || intent.kind != service_name(body.kind)
    {
        return Err(err_409("Prepared service identity differs from accepted setup"));
    }
    publish_planned_start(state.kv_client().await.as_ref(), &intent)
        .await
        .map_err(map_config_err)?;
    crate::services::remote::refresh(&state)
        .await
        .map_err(map_config_err)?;
    state.persist().map_err(|error| err_502(error.to_string()))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"operation_id":operation,"service_id":intent.service_id})),
    ))
}

async fn build(
    state: &AppState,
    binding: &NodeBinding,
    selected: &SetupService,
    operation: &str,
) -> Result<NodeServiceIntent, Failure> {
    let node: NodeEntry = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .node(binding.node_id)
        .cloned()
        .ok_or_else(|| err_409("Node configuration is not available"))?;
    let defaults = crate::services::defaults::get(State(state.clone())).await?.0;
    let public_name = serde_json::to_value(selected.kind).map_err(|error| err_502(error.to_string()))?;
    let mut choices = defaults[public_name
        .as_str()
        .ok_or_else(|| err_400("Invalid service kind"))?]
    .clone();
    let overrides = serde_json::to_value(&selected.listeners).map_err(|error| err_502(error.to_string()))?;
    for (key, value) in overrides
        .as_object()
        .ok_or_else(|| err_400("Invalid listener choices"))?
    {
        if !value.is_null() {
            choices[key] = value.clone();
        }
    }
    let name = service_name(selected.kind);
    let id = format!("{name}-{}", node.id);
    let (configuration, environment) = if selected.kind == SetupServiceKind::Diskdb {
        let body: crate::diskdb::DeployDiskdbBody =
            serde_json::from_value(choices).map_err(|error| err_400(error.to_string()))?;
        (
            crate::diskdb::planned_configuration(&node, &binding.management_seeds, &body)?,
            std::collections::BTreeMap::default(),
        )
    } else {
        if selected.kind == SetupServiceKind::PaxosKv {
            return Err(err_409("Admitted KV is owned by bootstrap"));
        }
        choices["kind"] = public_name;
        choices["instance_id"] = json!(node.id);
        choices
            .as_object_mut()
            .expect("default service choices")
            .remove("keepalive_interval_secs");
        if selected.kind != SetupServiceKind::Chunkdb {
            choices
                .as_object_mut()
                .expect("default service choices")
                .remove("dynamic_ownership");
        }
        if selected.kind == SetupServiceKind::ChunkKv {
            metadata(state, binding, &mut choices).await?;
        }
        let mut body: super::Deploy =
            serde_json::from_value(choices).map_err(|error| err_400(error.to_string()))?;
        super::policy::apply(state, &mut body)?;
        super::validate(&body)?;
        if matches!(
            selected.kind,
            SetupServiceKind::Chunkdb | SetupServiceKind::ChunkKv
        ) {
            if let Some(reason) = crate::services::dependencies::storage_wait_reason(state).await {
                return Err(err_409(reason));
            }
            super::geometry::validate(state).await?;
        }
        if selected.kind == SetupServiceKind::ChunkKv {
            if let Some(reason) = crate::services::dependencies::chunk_kv_wait_reason(state).await {
                return Err(err_409(reason));
            }
        }
        if selected.kind == SetupServiceKind::AccessServer {
            super::remote::prepare_credentials(state, binding, &node, &body).await?;
        }
        let configuration =
            super::remote::configuration(state, &node, &id, &binding.management_seeds, &body).await?;
        let configuration = if selected.kind == SetupServiceKind::ChunkKv {
            catalog::configure(state, binding, configuration).await?
        } else {
            configuration
        };
        (configuration, super::remote::environment(&body, &node))
    };
    Ok(NodeServiceIntent {
        cluster_id: binding.bootstrap.cluster_id.clone(),
        operation_id: operation.into(),
        node_id: node.id,
        service_id: id,
        kind: name.into(),
        action: NodeServiceAction::Start,
        configuration,
        environment,
    })
}

fn service_name(kind: SetupServiceKind) -> &'static str {
    match kind {
        SetupServiceKind::PaxosKv => "kv",
        SetupServiceKind::Diskdb => "diskdb",
        SetupServiceKind::Diskio => "diskio",
        SetupServiceKind::Chunkdb => "chunkdb",
        SetupServiceKind::ChunkKv => "chunk-kv",
        SetupServiceKind::AccessServer => "access",
    }
}

async fn metadata(state: &AppState, binding: &NodeBinding, choices: &mut Value) -> Result<(), Failure> {
    let ctx = state.op_context().await.map_err(map_config_err)?;
    let group = crowdb_console_shared::deployment::preparation::metadata_binding(&ctx, &binding.bootstrap)
        .await
        .map_err(map_config_err)?;
    choices["metadata_store_id"] = json!(group.store_id);
    Ok(())
}
