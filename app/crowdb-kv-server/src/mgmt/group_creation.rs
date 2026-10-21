// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable creation ownership precedes ordinary-group WAL and engine effects.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    Json,
};
use crowdb_protocol::mgmt::{AddGroupRequest, GroupCreationStatus};

use super::{err_json, system_bootstrap::ManagementError, RegistryArc};

pub(super) async fn status(
    State(state): State<RegistryArc>,
    AxumPath((store, group)): AxumPath<(u64, u64)>,
) -> Result<Json<GroupCreationStatus>, ManagementError> {
    read(&state.config.config_root, store, group)?
        .map(Json)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, "group creation ownership is absent"))
}

pub(super) async fn accept(
    state: &RegistryArc,
    store: u64,
    request: &AddGroupRequest,
) -> Result<(), ManagementError> {
    if store == 0 && request.group_id == 0 {
        return if request.deployment.is_none() {
            Ok(())
        } else {
            Err(err_json(
                StatusCode::BAD_REQUEST,
                "system group must use system bootstrap ownership",
            ))
        };
    }
    let expected = GroupCreationStatus {
        version: 1,
        store_id: store,
        group_id: request.group_id,
        replica_id: request.replica_id,
        deployment: request.deployment.clone(),
        retired: false,
    };
    validate(&expected)?;
    let root = &state.config.config_root;
    if let Some(current) = read(root, store, request.group_id)? {
        return matches(&current, &expected);
    }
    if state
        .get_store(store)
        .is_some_and(|store| store.get_group(request.group_id).is_some())
    {
        return if request.deployment.is_none() {
            Ok(())
        } else {
            Err(err_json(
                StatusCode::CONFLICT,
                "existing group has no matching deployment ownership",
            ))
        };
    }
    let persisted = crowdb_kv::cluster::node_config::NodeConfigStore::new(root)
        .load()
        .await
        .map_err(storage_error)?;
    if persisted
        .group(store, request.group_id)
        .is_some_and(|group| group.replica_id != request.replica_id)
    {
        return Err(err_json(
            StatusCode::CONFLICT,
            "persisted group replica identity differs",
        ));
    }
    if request.deployment.is_some()
        && (persisted.group(store, request.group_id).is_some()
            || crate::recovery::startup::store_crowdb_tree_path(
                &state.config.data_root,
                store,
                request.group_id,
            )
            .exists()
            || crate::recovery::startup::store_wal_root(&state.config.wal_root, store)
                .join(format!("group{}", request.group_id))
                .exists())
    {
        return Err(err_json(
            StatusCode::CONFLICT,
            "existing group has no matching deployment ownership",
        ));
    }
    fs::create_dir_all(root).map_err(storage_error)?;
    publish(root, &expected, false)?;
    let current = read(root, store, request.group_id)?
        .ok_or_else(|| err_json(StatusCode::CONFLICT, "group creation ownership disappeared"))?;
    matches(&current, &expected)
}

pub(super) fn retire(state: &RegistryArc, store: u64, group: u64) -> Result<(), ManagementError> {
    if let Some(mut current) = read(&state.config.config_root, store, group)? {
        if !current.retired {
            current.retired = true;
            publish(&state.config.config_root, &current, true)?;
        }
    }
    Ok(())
}

pub(super) fn finish_removal(state: &RegistryArc, store: u64, group: u64) -> Result<(), ManagementError> {
    let root = &state.config.config_root;
    if read(root, store, group)?.is_some_and(|current| current.deployment.is_none() && current.retired) {
        fs::remove_file(path(root, store, group)).map_err(storage_error)?;
        File::open(root)
            .and_then(|file| file.sync_all())
            .map_err(storage_error)?;
    }
    Ok(())
}

pub(crate) fn check_restore(root: &Path, store: u64, group: u64, replica: u64) -> std::io::Result<()> {
    let status = read(root, store, group).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid durable group creation ownership",
        )
    })?;
    if status.is_some_and(|status| status.retired || status.replica_id != replica) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "group creation is retired or has another replica identity",
        ));
    }
    Ok(())
}

fn matches(current: &GroupCreationStatus, expected: &GroupCreationStatus) -> Result<(), ManagementError> {
    if current != expected {
        return Err(err_json(
            StatusCode::CONFLICT,
            "group creation is retired or owned by another operation",
        ));
    }
    Ok(())
}

fn path(root: &Path, store: u64, group: u64) -> PathBuf {
    root.join(format!("group-creation-{store}-{group}.json"))
}

fn read(root: &Path, store: u64, group: u64) -> Result<Option<GroupCreationStatus>, ManagementError> {
    let path = path(root, store, group);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage_error(error)),
    };
    if !metadata.file_type().is_file()
        || metadata.len() > 4096
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(err_json(
            StatusCode::CONFLICT,
            "invalid durable group creation ownership",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(storage_error)?
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(storage_error)?;
    if bytes.len() > 4096 {
        return Err(err_json(
            StatusCode::CONFLICT,
            "group ownership exceeds its size bound",
        ));
    }
    let status: GroupCreationStatus = serde_json::from_slice(&bytes)
        .map_err(|_| err_json(StatusCode::CONFLICT, "invalid durable group creation ownership"))?;
    validate(&status)?;
    if status.store_id != store || status.group_id != group {
        return Err(err_json(
            StatusCode::CONFLICT,
            "group creation ownership path differs",
        ));
    }
    Ok(Some(status))
}

fn validate(status: &GroupCreationStatus) -> Result<(), ManagementError> {
    let valid_identity = status.deployment.as_ref().map_or(true, |identity| {
        [&identity.cluster_id, &identity.operation_id]
            .iter()
            .all(|value| {
                uuid::Uuid::parse_str(value).is_ok_and(|id| !id.is_nil() && id.to_string() == value.as_str())
            })
    });
    if status.version != 1 || status.replica_id == 0 || !valid_identity {
        return Err(err_json(StatusCode::CONFLICT, "invalid group creation identity"));
    }
    Ok(())
}

fn publish(root: &Path, status: &GroupCreationStatus, replace: bool) -> Result<(), ManagementError> {
    let temporary = root.join(format!(".group-creation-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(status).map_err(std::io::Error::other)?)?;
        file.sync_all()?;
        let target = path(root, status.store_id, status.group_id);
        if replace {
            fs::rename(&temporary, target)?;
        } else {
            match fs::hard_link(&temporary, target) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        File::open(root)?.sync_all()
    })();
    match fs::remove_file(temporary) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) if result.is_ok() => return Err(storage_error(error)),
        Ok(()) | Err(_) => {}
    }
    result.map_err(storage_error)
}

fn storage_error(error: impl std::fmt::Display) -> ManagementError {
    err_json(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("group ownership persistence failed: {error}"),
    )
}
