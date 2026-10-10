// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    error::{err_502, map_config_err, ErrorBody},
    state::AppState,
};
use axum::{http::StatusCode, Json};
use crowdb_console_shared::deployment::PreparedBootstrap;

/// Recover the operation accepted by the local monitor, independent of the initiating UI.
pub(super) fn recover(state: &AppState) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    if state.node_monitor_url.is_none() || state.runtime_root.join("prepared-bootstrap.json").exists() {
        return Ok(());
    }
    let Some(root) = state.runtime_root.parent() else {
        return Ok(());
    };
    let source = root.join("prepared-bootstrap.json");
    match std::fs::symlink_metadata(&source) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(err_502(error.to_string())),
    }
    let operation = PreparedBootstrap::load(&source).map_err(map_config_err)?;
    let credentials =
        crowdb_monitor::ServerCredentials::load_existing(root).map_err(|error| err_502(error.to_string()))?;
    crowdb_monitor::ServerCredentials::import(state.runtime_root.as_ref(), &credentials.server_env())
        .map_err(|error| err_502(error.to_string()))?;
    super::save(&state.runtime_root.join("prepared-bootstrap.json"), &operation)
}
