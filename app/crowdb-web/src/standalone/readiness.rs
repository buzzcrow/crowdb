// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Confirm discovery registrations before advertising recovered standalone readiness.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crowdb_console_shared::config::ServiceType;
use crowdb_console_shared::error::{Error, Result};
use crowdb_kv_client::ServiceRegistryClient;

use crate::state::AppState;

impl AppState {
    pub(super) async fn confirm_kv_registrations(&self) -> Result<()> {
        let expected: HashSet<_> = {
            let config = self
                .config
                .read()
                .map_err(|error| Error::Config(error.to_string()))?;
            config
                .servers
                .iter()
                .filter(|server| server.service_type == ServiceType::PaxosKv)
                .filter_map(|server| {
                    server
                        .node_id
                        .filter(|node| server.auto_start || self.runtime_pid(node).is_some())
                })
                .collect()
        };
        if expected.is_empty() {
            return Ok(());
        }
        let context = self.op_context().await?;
        let registry = ServiceRegistryClient::from_shared(context.kv_arc().clone());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let instances = registry
                .read_all_instances("kv-server")
                .await
                .map_err(|error| Error::Config(format!("recovered Group 0 discovery failed: {error}")))?;
            let mut counts = HashMap::<u64, usize>::new();
            for (_, instance) in instances {
                if let Some(node) = instance
                    .extra
                    .and_then(|extra| extra.kv_server)
                    .and_then(|server| server.node_id)
                {
                    *counts.entry(node).or_default() += 1;
                }
            }
            if expected.iter().all(|node| counts.get(node) == Some(&1)) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Config(format!("recovered KV registrations are missing or ambiguous: expected {expected:?}, observed {counts:?}")));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
