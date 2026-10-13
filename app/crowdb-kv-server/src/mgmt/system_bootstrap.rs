// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable system-store ownership, published before any store creation.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use crowdb_protocol::mgmt::{SystemBootstrapIdentity, SystemPrepareRequest};
use serde::{Deserialize, Serialize};

use super::{err_json, ErrorResponse, RegistryArc};

pub(super) type ManagementError = (StatusCode, Json<ErrorResponse>);
const OWNERSHIP_FILE: &str = "system-bootstrap.json";
const RETIRED_FILE: &str = "system-bootstrap-cleanup.json";
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptedBootstrap {
    version: u32,
    replica_id: u64,
    identity: SystemBootstrapIdentity,
}

pub(super) struct BootstrapExecution(Arc<crate::store_registry::KvStoreRegistry>);

impl Drop for BootstrapExecution {
    fn drop(&mut self) {
        self.0.bootstrap_executing.store(false, Ordering::Release);
    }
}

pub(super) fn begin(state: &RegistryArc) -> Result<BootstrapExecution, ManagementError> {
    state
        .bootstrap_executing
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .map_err(|_| {
            err_json(
                StatusCode::CONFLICT,
                "system bootstrap is executing; retry the same operation",
            )
        })?;
    Ok(BootstrapExecution(Arc::clone(&state.registry)))
}

pub(super) async fn prepare(
    State(state): State<RegistryArc>,
    Json(req): Json<SystemPrepareRequest>,
) -> Result<Json<SystemPrepareRequest>, ManagementError> {
    let _execution = begin(&state)?;
    accept(&state, req.replica_id, Some(&req.bootstrap))?;
    Ok(Json(req))
}

pub(super) fn has_owner(state: &RegistryArc) -> Result<bool, ManagementError> {
    match fs::symlink_metadata(state.config.config_root.join(OWNERSHIP_FILE)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(storage_error(&error)),
    }
}

pub(super) fn accept(
    state: &RegistryArc,
    replica_id: u64,
    identity: Option<&SystemBootstrapIdentity>,
) -> Result<(), ManagementError> {
    let root = &state.config.config_root;
    let path = root.join(OWNERSHIP_FILE);
    if root.join(RETIRED_FILE).exists() {
        return Err(err_json(StatusCode::CONFLICT, "system cleanup has not completed"));
    }
    let Some(identity) = identity else {
        return if has_owner(state)? {
            Err(err_json(
                StatusCode::CONFLICT,
                "system store belongs to an identified bootstrap operation",
            ))
        } else {
            Ok(())
        };
    };
    validate(replica_id, identity)?;
    if root
        .join(format!("system-bootstrap-retired-{}.json", identity.operation_id))
        .exists()
    {
        return Err(err_json(
            StatusCode::CONFLICT,
            "bootstrap operation was explicitly retired",
        ));
    }
    if root.join(RETIRED_FILE).exists() {
        return Err(err_json(StatusCode::CONFLICT, "system cleanup has not completed"));
    }
    let accepted = AcceptedBootstrap {
        version: 1,
        replica_id,
        identity: identity.clone(),
    };
    if has_owner(state)? {
        return verify(&path, &accepted);
    }
    if state.contains_store(0) {
        return Err(err_json(
            StatusCode::CONFLICT,
            "existing system store has no matching bootstrap ownership",
        ));
    }
    fs::create_dir_all(root).map_err(|error| storage_error(&error))?;
    let temporary = root.join(format!(
        ".system-bootstrap-{}-{}.tmp",
        std::process::id(),
        NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|error| storage_error(&error))?;
        let bytes =
            serde_json::to_vec(&accepted).map_err(|error| storage_error(&std::io::Error::other(error)))?;
        file.write_all(&bytes).map_err(|error| storage_error(&error))?;
        file.sync_all().map_err(|error| storage_error(&error))?;
        match fs::hard_link(&temporary, &path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(storage_error(&error)),
        }
        File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| storage_error(&error))?;
        verify(&path, &accepted)
    })();
    let cleanup = fs::remove_file(&temporary);
    if result.is_ok() {
        cleanup.map_err(|error| storage_error(&error))?;
    }
    result
}

fn validate(replica_id: u64, identity: &SystemBootstrapIdentity) -> Result<(), ManagementError> {
    fn uuid(text: &str) -> bool {
        text.len() == 36
            && text.bytes().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) {
                    byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            })
            && text
                .bytes()
                .any(|byte| matches!(byte, b'1'..=b'9' | b'a'..=b'f' | b'A'..=b'F'))
    }
    if replica_id == 0
        || !uuid(&identity.cluster_id)
        || !uuid(&identity.operation_id)
        || identity.configuration_digest.len() != 64
        || !identity
            .configuration_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(err_json(
            StatusCode::BAD_REQUEST,
            "bootstrap requires nonzero replica, cluster/operation UUIDs and SHA-256 configuration digest",
        ));
    }
    Ok(())
}

fn verify(path: &Path, expected: &AcceptedBootstrap) -> Result<(), ManagementError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| storage_error(&error))?;
    if !metadata.file_type().is_file()
        || metadata.len() > 4096
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(err_json(
            StatusCode::CONFLICT,
            "invalid durable system bootstrap ownership",
        ));
    }
    let mut bytes = Vec::with_capacity(4096);
    File::open(path)
        .map_err(|error| storage_error(&error))?
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|error| storage_error(&error))?;
    let actual: AcceptedBootstrap = serde_json::from_slice(&bytes)
        .map_err(|_| err_json(StatusCode::CONFLICT, "invalid durable system bootstrap ownership"))?;
    if &actual != expected {
        return Err(err_json(
            StatusCode::CONFLICT,
            "system store is reserved by a conflicting bootstrap operation",
        ));
    }
    Ok(())
}

fn storage_error(error: &std::io::Error) -> ManagementError {
    err_json(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("system bootstrap persistence failed: {error}"),
    )
}

mod cleanup;
pub(super) use cleanup::cleanup;

/// # Errors
/// Reports unreadable cleanup markers instead of restarting a retired system store.
pub fn system_cleanup_pending(root: &Path) -> std::io::Result<bool> {
    match fs::symlink_metadata(root.join(RETIRED_FILE)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
