// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable deployment progress. Browser ownership uses revision compare-and-set;
//! interrupted deployment is reconciled against registered instances before retry.

use std::{collections::BTreeMap, path::PathBuf};

use axum::{
    extract::{Path, State},
    Json,
};
use serde::{Deserialize, Serialize};

use super::{operation::Operation, Failure};
use crate::{
    error::{err_400, err_404, err_409, err_500},
    state::AppState,
};

const KINDS: [&str; 6] = ["kv", "diskdb", "chunkdb", "diskio", "chunk-kv", "access-server"];
const MAX_BYTES: u64 = 32 * 1024;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Step {
    state: StepState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum StepState {
    Pending,
    Waiting,
    Deploying,
    Deployed,
    Failed,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    revision: u32,
    steps: BTreeMap<String, Step>,
}

fn path(state: &AppState, id: u64) -> PathBuf {
    state.node_workspace_dir(id).join("service-plan.json")
}

fn read(state: &AppState, id: u64) -> Result<Option<Plan>, Failure> {
    let path = path(state, id);
    let metadata = match std::fs::metadata(&path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(err_500(format!("Read deployment plan: {error}"))),
    };
    if metadata.len() > MAX_BYTES {
        return Err(err_500("Stored deployment plan exceeds its limit"));
    }
    let bytes = std::fs::read(path).map_err(|error| err_500(format!("Read deployment plan: {error}")))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| err_500(format!("Decode deployment plan: {error}")))
}

pub(super) async fn list(State(state): State<AppState>) -> Result<Json<BTreeMap<u64, Plan>>, Failure> {
    let nodes: Vec<_> = state
        .config
        .read()
        .unwrap()
        .nodes
        .iter()
        .map(|node| node.id)
        .collect();
    if nodes.len() > 1000 {
        return Err(err_400("Deployment plan inventory exceeds 1000 nodes"));
    }
    let mut plans = BTreeMap::new();
    for id in nodes {
        if let Some(plan) = read(&state, id)? {
            plans.insert(id, plan);
        }
    }
    Ok(Json(plans))
}

pub(super) async fn put(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Json(mut plan): Json<Plan>,
) -> Result<Json<Plan>, Failure> {
    let _operation = Operation::claim(&state, vec![format!("node/{id}")])?;
    if !state
        .config
        .read()
        .unwrap()
        .nodes
        .iter()
        .any(|node| node.id == id)
    {
        return Err(err_404("Deployment plan node no longer exists"));
    }
    if plan.steps.len() != KINDS.len()
        || KINDS.iter().any(|kind| !plan.steps.contains_key(*kind))
        || plan
            .steps
            .values()
            .any(|step| step.detail.as_ref().is_some_and(|detail| detail.len() > 4096))
    {
        return Err(err_400(
            "Deployment plan requires exactly six bounded service steps",
        ));
    }
    let revision = read(&state, id)?.map_or(0, |value| value.revision);
    if plan.revision != revision {
        return Err(err_409("Deployment plan changed; reload before resuming"));
    }
    plan.revision = revision
        .checked_add(1)
        .ok_or_else(|| err_409("Deployment plan revision exhausted"))?;
    let bytes =
        serde_json::to_vec(&plan).map_err(|error| err_500(format!("Encode deployment plan: {error}")))?;
    let target = path(&state, id);
    let parent = target.parent().unwrap();
    let persist = || -> std::io::Result<()> {
        use std::io::Write;
        std::fs::create_dir_all(parent)?;
        let temporary = target.with_extension("json.tmp");
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(temporary, &target)?;
        std::fs::File::open(parent)?.sync_all()
    };
    persist().map_err(|error| {
        err_500(format!(
            "Persist deployment plan: {error}; reload to reconcile the outcome"
        ))
    })?;
    Ok(Json(plan))
}

pub(super) fn remove(state: &AppState, id: u64) -> Result<(), Failure> {
    match std::fs::remove_file(path(state, id)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(err_500(format!("Remove deployment plan: {error}"))),
    }
}
