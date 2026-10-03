// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::Failure;
use crate::{error::err_500, state::AppState};

/// A newly started child may run only after its recovery inputs are durable.
pub(super) async fn publish(state: &AppState, id: &str, pid: u32) -> Result<(), Failure> {
    let Err(error) = state.persist() else {
        return Ok(());
    };
    let stopped = tokio::task::spawn_blocking(move || crowdb_console_shared::lifecycle::stop_pid(pid))
        .await
        .map_err(|join| {
            err_500(format!(
                "Deployment publication failed ({error}); cleanup task failed: {join}"
            ))
        })?;
    stopped.map_err(|stop| {
        err_500(format!(
            "Deployment publication failed ({error}); child {pid} could not stop: {stop}"
        ))
    })?;
    {
        let mut config = state.config.write().unwrap();
        if let Some(entry) = config.servers.iter_mut().find(|entry| entry.id == id) {
            entry.pid = None;
            entry.auto_start = false;
        }
    }
    // Retain the launch and workspace for a deliberate retry when storage recovers.
    if let Err(recovery) = state.persist() {
        tracing::error!(service = id, %recovery, "stopped deployment retained only in memory after publication failure");
    }
    Err(err_500(format!(
        "Deployment publication failed; child stopped and workspace preserved: {error}"
    )))
}
