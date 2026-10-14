// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bound-node deletions use Group-0 authority instead of local draft cleanup.

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
};

use crate::{error::err_400, managed_hardware, state::AppState};

pub(super) async fn delete(state: &AppState, path: &str) -> Option<Response> {
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    match parts.as_slice() {
        ["api", "nodes", node] => {
            let Ok(node) = node.parse::<u64>() else {
                return Some(err_400("Invalid node ID").into_response());
            };
            Some(
                managed_hardware::remove_node(State(state.clone()), Path(node))
                    .await
                    .into_response(),
            )
        }
        ["api", "nodes", node, "disk-groups", group] => {
            let (Ok(node), Ok(group)) = (node.parse::<u64>(), group.parse::<u64>()) else {
                return Some(err_400("Invalid storage placement").into_response());
            };
            Some(
                managed_hardware::remove_disk_group(State(state.clone()), Path((node, group)))
                    .await
                    .into_response(),
            )
        }
        ["api", "nodes", node, "disk-groups", group, "disks", disk] => {
            let (Ok(node), Ok(group)) = (node.parse::<u64>(), group.parse::<u64>()) else {
                return Some(err_400("Invalid storage placement").into_response());
            };
            Some(
                managed_hardware::remove_disk(State(state.clone()), Path((node, group, (*disk).into())))
                    .await
                    .into_response(),
            )
        }
        _ => None,
    }
}
