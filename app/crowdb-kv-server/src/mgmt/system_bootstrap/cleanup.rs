// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    begin, err_json, has_owner, storage_error, validate, verify, AcceptedBootstrap, ManagementError,
    RegistryArc, OWNERSHIP_FILE, RETIRED_FILE,
};
use axum::{extract::State, http::StatusCode, Json};
use crowdb_protocol::mgmt::SystemBootstrapIdentity;
use serde::Deserialize;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;

#[derive(Deserialize)]
pub(crate) struct CleanupRequest {
    bootstrap: SystemBootstrapIdentity,
    confirm_delete_system_store: bool,
}

pub(crate) async fn cleanup(
    State(state): State<RegistryArc>,
    Json(request): Json<CleanupRequest>,
) -> Result<StatusCode, ManagementError> {
    let _execution = begin(&state)?;
    if !request.confirm_delete_system_store {
        return Err(err_json(
            StatusCode::BAD_REQUEST,
            "explicit system-store deletion confirmation required",
        ));
    }
    validate(1, &request.bootstrap)?;
    let root = &state.config.config_root;
    let retired = root.join(format!(
        "system-bootstrap-retired-{}.json",
        request.bootstrap.operation_id
    ));
    if !has_owner(&state)? {
        if retired.exists() {
            finish_retired(root, &retired, &request.bootstrap)?;
            return Ok(StatusCode::OK);
        }
        if state.contains_store(0) {
            return Err(err_json(
                StatusCode::CONFLICT,
                "existing system store has no matching ownership",
            ));
        }
        fs::create_dir_all(root).map_err(|error| storage_error(&error))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&retired)
            .map_err(|error| storage_error(&error))?;
        file.write_all(
            &serde_json::to_vec(&request.bootstrap)
                .map_err(|error| storage_error(&std::io::Error::other(error)))?,
        )
        .and_then(|()| file.sync_all())
        .map_err(|error| storage_error(&error))?;
        File::open(root)
            .and_then(|file| file.sync_all())
            .map_err(|error| storage_error(&error))?;
        return Ok(StatusCode::OK);
    }
    let bytes = fs::read(root.join(OWNERSHIP_FILE)).map_err(|error| storage_error(&error))?;
    let accepted: AcceptedBootstrap =
        serde_json::from_slice(&bytes).map_err(|error| storage_error(&std::io::Error::other(error)))?;
    verify(&root.join(OWNERSHIP_FILE), &accepted)?;
    if accepted.identity != request.bootstrap {
        return Err(err_json(
            StatusCode::CONFLICT,
            "cleanup belongs to another bootstrap operation",
        ));
    }
    if !retired.exists() {
        fs::hard_link(root.join(OWNERSHIP_FILE), &retired).map_err(|error| storage_error(&error))?;
    }
    if !root.join(RETIRED_FILE).exists() {
        fs::hard_link(&retired, root.join(RETIRED_FILE)).map_err(|error| storage_error(&error))?;
    }
    File::open(root)
        .and_then(|file| file.sync_all())
        .map_err(|error| storage_error(&error))?;
    if state.contains_store(0) {
        super::super::store_ops::remove_store(State(state.clone()), axum::extract::Path(0)).await?;
    }
    let node_config = crowdb_kv::cluster::node_config::NodeConfigStore::new(root);
    node_config
        .remove_store(0)
        .await
        .map_err(|error| storage_error(&error))?;
    for path in [
        state.config.data_root.join("store0"),
        crate::recovery::startup::store_wal_root(&state.config.wal_root, 0),
    ] {
        match fs::remove_dir_all(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage_error(&error)),
        }
    }
    fs::remove_file(root.join(OWNERSHIP_FILE)).map_err(|error| storage_error(&error))?;
    File::open(root)
        .and_then(|file| file.sync_all())
        .map_err(|error| storage_error(&error))?;
    fs::remove_file(root.join(RETIRED_FILE)).map_err(|error| storage_error(&error))?;
    File::open(root)
        .and_then(|file| file.sync_all())
        .map_err(|error| storage_error(&error))?;
    Ok(StatusCode::OK)
}

fn finish_retired(
    root: &std::path::Path,
    retired: &std::path::Path,
    bootstrap: &SystemBootstrapIdentity,
) -> Result<(), ManagementError> {
    if root.join(RETIRED_FILE).exists() {
        let accepted: AcceptedBootstrap =
            serde_json::from_slice(&fs::read(retired).map_err(|error| storage_error(&error))?)
                .map_err(|error| storage_error(&std::io::Error::other(error)))?;
        if accepted.identity != *bootstrap {
            return Err(err_json(
                StatusCode::CONFLICT,
                "cleanup belongs to another bootstrap operation",
            ));
        }
        verify(&root.join(RETIRED_FILE), &accepted)?;
        fs::remove_file(root.join(RETIRED_FILE)).map_err(|error| storage_error(&error))?;
        File::open(root)
            .and_then(|file| file.sync_all())
            .map_err(|error| storage_error(&error))?;
    }
    Ok(())
}
