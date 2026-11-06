// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{err_json, ErrorResponse, RegistryArc};
use axum::{http::StatusCode, Json};
use crowdb_paxos_kv::cluster::group::PxGroup;
use std::time::Duration;

pub(super) async fn before_group_removal(
    state: &RegistryArc,
    group: &PxGroup,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if !group.local_replica().learner.engine().requires_contiguous_apply() {
        return Ok(());
    }
    let report = group
        .shutdown(Duration::from_millis(state.config.server.shutdown_timeout_ms))
        .await;
    if report.is_clean() {
        return Ok(());
    }
    Err(err_json(
        StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "runtime group retained; shutdown incomplete: {}",
            report.errors.join("; ")
        ),
    ))
}
