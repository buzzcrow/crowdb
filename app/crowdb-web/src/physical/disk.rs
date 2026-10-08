// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Merge newly provisioned disks without replacing concurrent service updates.

use axum::{http::StatusCode, Json};
use crowdb_console_shared::config::DiskEntry;

use crate::{
    error::{map_config_err, map_persist_err, ErrorBody},
    state::AppState,
};

pub(crate) fn persist_added(
    state: &AppState,
    entries: &[DiskEntry],
) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    {
        let mut config = state.config.write().unwrap();
        let mut updated = config.clone();
        for entry in entries {
            updated.add_disk(entry.clone()).map_err(map_config_err)?;
        }
        *config = updated;
    }
    state.persist().map_err(map_persist_err)
}
