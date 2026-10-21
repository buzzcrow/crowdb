// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Candidate policy checks precede local credential use and admission effects.

use axum::{extract::State, Json};
use crowdb_protocol::mgmt::node::{
    deployment::{DeploymentMode, NodeDeploymentPolicy},
    CandidateNode, CandidateState,
};
use std::{io::Read, os::unix::fs::PermissionsExt};

use super::Failure;
use crate::{
    error::{err_409, err_502},
    node::NodeCandidates,
    state::AppState,
};

pub(super) fn candidates<'a>(
    snapshot: &'a NodeCandidates,
    policy: &NodeDeploymentPolicy,
) -> Result<Vec<&'a CandidateNode>, Failure> {
    let expected = policy.formation();
    if snapshot.local.advertisement.formation.as_ref() != Some(&expected) {
        return Err(err_409(
            "Local monitor formation policy differs from accepted startup inputs",
        ));
    }
    let local_id = &snapshot.local.advertisement.discovery_id;
    if snapshot.candidates.iter().any(|peer| {
        peer.state == CandidateState::IdentityConflict
            && (peer.advertisement.discovery_id == *local_id
                || peer.advertisement.formation.as_ref() == Some(&expected))
    }) {
        return Err(err_409(
            "Conflicting live node identities prevent automatic formation",
        ));
    }
    let mut candidates: Vec<_> = snapshot
        .candidates
        .iter()
        .filter(|peer| {
            peer.state == CandidateState::Unbound
                && peer.advertisement.formation.as_ref() == Some(&expected)
                && (policy.initial_voters != 1 || peer.advertisement.discovery_id == *local_id)
        })
        .collect();
    candidates.sort_by(|left, right| {
        left.advertisement
            .discovery_id
            .cmp(&right.advertisement.discovery_id)
    });
    if candidates.len() > usize::from(policy.initial_voters) {
        return Err(err_409(
            "Too many fresh nodes in the cluster scope; select an unambiguous formation",
        ));
    }
    if policy.initial_voters == 3
        && snapshot.candidates.iter().any(|peer| {
            peer.state == CandidateState::ForeignCluster
                && peer.advertisement.formation.as_ref() == Some(&expected)
        })
    {
        return Err(err_409(
            "Cluster scope already has bound members; use explicit admission to join it",
        ));
    }
    Ok(candidates)
}

pub(super) async fn prepare_local_rack(
    state: &AppState,
    policy: &NodeDeploymentPolicy,
) -> Result<(), Failure> {
    let has_rack = !state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .racks
        .is_empty();
    if policy.mode == DeploymentMode::Local && !has_rack {
        let request = serde_json::from_value(serde_json::json!({"id":1,"name":"local cluster"}))
            .map_err(|error| err_502(error.to_string()))?;
        let _ = crate::lifecycle::http_add_rack(State(state.clone()), Json(request)).await?;
    }
    Ok(())
}

pub(super) fn local_password(state: &AppState) -> Result<String, Failure> {
    let root = state
        .runtime_root
        .parent()
        .ok_or_else(|| err_409("Node data root is absent"))?;
    let path = root.join("ssh/local-admission-password");
    let metadata = std::fs::symlink_metadata(&path).map_err(|_| {
        err_409(
            "Local admission credential is unavailable; prepare the node through its container entrypoint",
        )
    })?;
    if !metadata.file_type().is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.len() == 0
        || metadata.len() > 4096
    {
        return Err(err_409(
            "Local admission credential must be a bounded private regular file",
        ));
    }
    let mut password = String::new();
    std::fs::File::open(path)
        .map_err(|_| err_502("Cannot read local admission credential"))?
        .take(4097)
        .read_to_string(&mut password)
        .map_err(|_| err_502("Cannot read local admission credential"))?;
    if password.is_empty() || password.len() > 4096 {
        return Err(err_409("Invalid local admission credential length"));
    }
    Ok(password)
}

pub(super) fn waiting_for_peers(
    state: &AppState,
    snapshot: &NodeCandidates,
    selected: &[&CandidateNode],
    policy: &NodeDeploymentPolicy,
) -> Result<Option<serde_json::Value>, Failure> {
    if selected.len() < usize::from(policy.initial_voters) {
        let records = crate::node::records(state)?;
        let config = state.config.read().map_err(|error| err_502(error.to_string()))?;
        let admitted: Vec<_> = selected
            .iter()
            .filter_map(|candidate| {
                records
                    .iter()
                    .find(|record| {
                        !record.cancelled
                            && record.discovery_id == candidate.advertisement.discovery_id
                            && config.node(record.node_id).is_some()
                    })
                    .map(|record| record.node_id)
            })
            .collect();
        return Ok(Some(
            serde_json::json!({"phase":"waiting","required_voters":policy.initial_voters,
            "members":admitted,"observed_voters":selected.len(),"reason":"Waiting for the complete initial formation before admission"}),
        ));
    }
    if let Some(coordinator) = selected
        .first()
        .filter(|candidate| candidate.advertisement.discovery_id != snapshot.local.advertisement.discovery_id)
    {
        return Ok(Some(
            serde_json::json!({"phase":"waiting","required_voters":policy.initial_voters,
            "coordinator":coordinator.advertisement.discovery_id,"members":[],
            "reason":"Waiting for the UUID-ordered formation coordinator"}),
        ));
    }
    Ok(None)
}
