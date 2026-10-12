// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Live topology observations for monitor-owned remote KV processes.

use crate::state::AppState;
use crowdb_console_shared::config::ServiceType;

pub(crate) async fn refresh(state: &AppState) {
    if state.node_monitor_url.is_none() {
        return;
    }
    let nodes: Vec<_> = {
        let config = state.config.read().unwrap();
        config
            .servers
            .iter()
            .filter_map(|server| {
                (server.service_type == ServiceType::PaxosKv)
                    .then_some(server.node_id)
                    .flatten()
            })
            .collect()
    };
    futures::future::join_all(
        nodes
            .into_iter()
            .map(|node| crate::mgmt::refresh_node_cache(state, node)),
    )
    .await;
}
