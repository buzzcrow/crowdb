// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit candidate admission into an existing, compatible node `DiskGroup`.

mod request;
mod view;

use super::{remote_node, Failure};
use crate::{
    error::{err_400, err_409, err_502, map_config_err},
    state::AppState,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use crowdb_console_shared::deployment::{admission::remote_control, registry};
use crowdb_protocol::mgmt::node::{
    setup::SetupMedium,
    storage::{DiskInventory, PrepareSuppliedDisk, SuppliedPreparation, SuppliedPreparationPage},
    NodeControl,
};
use serde::Deserialize;
use view::{CandidateOutcome, Page};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JoinCandidate {
    operation_id: String,
    disk_group_id: u64,
    medium: SetupMedium,
}
#[derive(Deserialize)]
pub(crate) struct PageQuery {
    after: Option<String>,
    #[serde(default = "page_limit")]
    limit: u32,
}
fn page_limit() -> u32 {
    16
}

pub(crate) async fn create(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<JoinCandidate>,
) -> Result<(StatusCode, Json<CandidateOutcome>), Failure> {
    let operation = canonical(&body.operation_id)?;
    if !body.medium.initialize || body.medium.resource_identity.is_none() {
        return Err(err_400(
            "Join requires explicit initialization consent and reviewed resource identity",
        ));
    }
    let target = remote_node(&state, node_id)?;
    let binding =
        crate::node::cluster_binding(&state)?.ok_or_else(|| err_409("Join is waiting for Group 0"))?;
    let previous: Option<SuppliedPreparation> = serde_json::from_value(
        remote_control(
            &target,
            &NodeControl::SuppliedDiskPreparation {
                operation_id: body.operation_id.clone(),
            },
        )
        .await
        .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    let request = if let Some(previous) = previous {
        let view = CandidateOutcome::from_state(&previous)?;
        if previous.request.cluster_id != binding.bootstrap.cluster_id
            || view.node_id != node_id
            || view.disk_group_id != body.disk_group_id
            || view.medium != body.medium
        {
            return Err(err_409(
                "Accepted candidate operation inputs changed; retain its original placement and medium",
            ));
        }
        previous.request
    } else {
        prepare(
            &state,
            node_id,
            &target,
            &binding.bootstrap.cluster_id,
            operation,
            &body,
        )
        .await?
    };
    let accepted: SuppliedPreparation = serde_json::from_value(
        remote_control(
            &target,
            &NodeControl::PrepareSuppliedDisk {
                request: request.clone(),
            },
        )
        .await
        .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    if accepted.request != request {
        return Err(err_502("Target returned a different accepted candidate intent"));
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(CandidateOutcome::from_state(&accepted)?),
    ))
}

async fn prepare(
    state: &AppState,
    node_id: u64,
    target: &crowdb_console_shared::config::NodeEntry,
    cluster: &str,
    operation: uuid::Uuid,
    body: &JoinCandidate,
) -> Result<PrepareSuppliedDisk, Failure> {
    let (hardware, rack) = super::authority::node(state, node_id).await?;
    if rack != target.rack_id {
        return Err(err_409("Target node placement changed"));
    }
    hardware
        .validate_disk_group_storage(
            rack,
            node_id,
            body.disk_group_id,
            body.medium.media_type,
            1024 * 1024,
        )
        .await
        .map_err(|error| err_409(error.to_string()))?;
    let placement = hardware
        .get_bind(rack, node_id, body.disk_group_id)
        .await
        .map_err(|error| err_502(error.to_string()))?;
    if !placement.is_some_and(|binding| binding.group_id != 0) {
        return Err(err_409("Select a DiskGroup bound to an ordinary metadata group"));
    }
    let registry = registry::read(state.kv_client().await.as_ref())
        .await
        .map_err(|error| err_502(error.to_string()))?
        .filter(|(registry, _)| registry.cluster_id == cluster)
        .ok_or_else(|| err_409("Confirmed node registry unavailable"))?
        .0;
    let owner = registry
        .nodes
        .iter()
        .find(|node| node.node_id == node_id && node.rack_id == rack && node.confirmed && !node.cancelled)
        .ok_or_else(|| err_409("Confirmed target node identity unavailable"))?;
    let inventory: DiskInventory = serde_json::from_value(
        remote_control(target, &NodeControl::DiskInventory)
            .await
            .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    let medium = &body.medium;
    let observed = inventory
        .disks
        .iter()
        .find(|disk| {
            disk.path == medium.path
                && disk.supplied
                && disk.error.is_none()
                && disk.backend == Some(medium.backend)
                && disk.backing_identity.as_ref() == Some(&medium.backing_identity)
                && disk.resource_identity == medium.resource_identity
                && disk.capacity_bytes == Some(medium.capacity_bytes)
        })
        .ok_or_else(|| {
            err_409("Reviewed supplied medium changed or is ineligible; refresh and review again")
        })?;
    request::prepare(cluster, owner, operation, body, observed)
}

pub(crate) async fn list(
    State(state): State<AppState>,
    Path(node): Path<u64>,
    Query(query): Query<PageQuery>,
) -> Result<Json<Page>, Failure> {
    if query.limit == 0 || query.limit > 16 {
        return Err(err_400("Candidate operation page limit must be 1..16"));
    }
    if let Some(after) = &query.after {
        canonical(after)?;
    }
    let target = remote_node(&state, node)?;
    let page: SuppliedPreparationPage = serde_json::from_value(
        remote_control(
            &target,
            &NodeControl::SuppliedDiskPreparations {
                after: query.after,
                limit: query.limit,
            },
        )
        .await
        .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    if page.items.len() > query.limit as usize
        || page.items.iter().any(|item| item.request.record.node_id != node)
    {
        return Err(err_502("Target returned invalid candidate operation coverage"));
    }
    Ok(Json(Page {
        items: page
            .items
            .iter()
            .map(CandidateOutcome::from_state)
            .collect::<Result<_, _>>()?,
        next: page.next,
    }))
}

pub(crate) async fn status(
    State(state): State<AppState>,
    Path((node, id)): Path<(u64, String)>,
) -> Result<Json<Option<CandidateOutcome>>, Failure> {
    canonical(&id)?;
    let target = remote_node(&state, node)?;
    let current: Option<SuppliedPreparation> = serde_json::from_value(
        remote_control(
            &target,
            &NodeControl::SuppliedDiskPreparation {
                operation_id: id.clone(),
            },
        )
        .await
        .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    if current
        .as_ref()
        .is_some_and(|item| item.request.record.node_id != node || item.request.record.operation_id != id)
    {
        return Err(err_502("Target returned another candidate operation"));
    }
    Ok(Json(
        current.as_ref().map(CandidateOutcome::from_state).transpose()?,
    ))
}
fn canonical(id: &str) -> Result<uuid::Uuid, Failure> {
    let value = uuid::Uuid::parse_str(id).map_err(|error| err_400(error.to_string()))?;
    if value.is_nil() || value.to_string() != id {
        return Err(err_400("Operation must be a canonical non-nil UUID"));
    }
    Ok(value)
}
