// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::mgmt::node::deployment::DeploymentMode;

use super::Deploy;
use crate::{
    error::{err_409, err_502},
    services::Failure,
    state::AppState,
};

pub(super) fn apply(state: &AppState, body: &mut Deploy) -> Result<(), Failure> {
    if state.node_monitor_url.is_some() {
        let root = state
            .runtime_root
            .parent()
            .ok_or_else(|| err_409("Node deployment policy root is absent"))?;
        if let Some(policy) =
            crowdb_monitor::read_node_policy(root).map_err(|error| err_502(error.to_string()))?
        {
            let local = policy.mode == DeploymentMode::Local;
            if body.local() && !local {
                return Err(err_409(
                    "Production deployment cannot use local service placement",
                ));
            }
            body.local_deployment = Some(body.local_deployment.unwrap_or(local));
            if body.local() && body.local_mirror_copies.is_none() {
                body.local_mirror_copies = Some(if policy.initial_voters == 1 { 1 } else { 2 });
            }
        }
    }
    Ok(())
}
