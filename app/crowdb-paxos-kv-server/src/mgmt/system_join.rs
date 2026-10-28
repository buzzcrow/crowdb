// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reserve system resources before importing a joining Group 0 replica.

use super::{err_json, system_bootstrap::ManagementError, RegistryArc};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crowdb_protocol::mgmt::JoinGroupRequest;

pub(super) async fn join(
    State(state): State<RegistryArc>,
    Json(request): Json<JoinGroupRequest>,
) -> Result<StatusCode, ManagementError> {
    let _execution = super::system_bootstrap::begin(&state)?;
    super::system_bootstrap::accept(&state, request.replica_id, request.bootstrap.as_ref())?;
    let store = super::system_store::ensure(&state).await?;
    if let Some(group) = store.get_group(0) {
        return if group.local_replica().id == request.replica_id && request.bootstrap.is_some() {
            Ok(StatusCode::OK)
        } else {
            Err(err_json(StatusCode::CONFLICT, "existing system replica differs"))
        };
    }
    super::group_ops::join_snapshot(State(state), Path((0, 0)), Json(request)).await
}
