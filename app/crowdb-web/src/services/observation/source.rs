// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated process and real endpoint transports for inventory sampling.

use crate::state::AppState;
use crowdb_console_shared::{
    cluster::NodeHealth,
    config::{NodeEntry, ServerEntry},
    ConsoleConfig,
};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeControl, NodeServiceIntent};
use serde_json::Value;

/// One node sampler owns this reader throughout its bound configuration.
#[async_trait::async_trait]
pub trait ProcessObservation: Send {
    /// Read one fresh monitor response; never return a retained observation.
    async fn read(&mut self) -> Result<Value, String>;
}

#[async_trait::async_trait]
impl ProcessObservation for crowdb_console_shared::deployment::admission::RemoteProcessObservation {
    async fn read(&mut self) -> Result<Value, String> {
        Self::read(self).await.map_err(|error| error.to_string())
    }
}

/// Observation transports; injected only by focused regression fixtures.
#[async_trait::async_trait]
pub trait ServiceObservationSource: Send + Sync + 'static {
    /// Create an owned process transport when the source supports connection reuse.
    ///
    /// # Errors
    /// Rejects an invalid bound transport configuration.
    fn process_observer(
        &self,
        _state: &AppState,
        _binding: &NodeBinding,
        _node: &NodeEntry,
    ) -> Result<Option<Box<dyn ProcessObservation>>, String> {
        Ok(None)
    }
    /// Read the committed service view without accepting mutations.
    async fn intents(&self, state: &AppState) -> Result<Vec<NodeServiceIntent>, String>;
    /// Read authenticated, bound monitor-owned children.
    async fn processes(
        &self,
        state: &AppState,
        binding: &NodeBinding,
        node: &NodeEntry,
    ) -> Result<Value, String>;
    /// Probe the service's actual endpoint, independently of process presence.
    async fn health(
        &self,
        state: &AppState,
        config: &ConsoleConfig,
        entry: &ServerEntry,
        pid: Option<u32>,
        intents: &[NodeServiceIntent],
    ) -> NodeHealth;
}

pub(super) struct Production;
#[async_trait::async_trait]
impl ServiceObservationSource for Production {
    fn process_observer(
        &self,
        state: &AppState,
        binding: &NodeBinding,
        node: &NodeEntry,
    ) -> Result<Option<Box<dyn ProcessObservation>>, String> {
        if node.id == binding.node_id {
            return Ok(None);
        }
        let mut target = node.clone();
        target.ssh_password = None;
        target.ssh_key = Some(crate::node::node_key_path(state).to_string_lossy().into_owned());
        let observer = crowdb_console_shared::deployment::admission::RemoteProcessObservation::new(
            target,
            binding.clone(),
        )
        .map_err(|error| error.to_string())?;
        Ok(Some(Box::new(observer)))
    }
    async fn intents(&self, state: &AppState) -> Result<Vec<NodeServiceIntent>, String> {
        crowdb_console_shared::deployment::services::list(state.kv_client().await.as_ref())
            .await
            .map_err(|error| error.to_string())
    }
    async fn processes(
        &self,
        state: &AppState,
        binding: &NodeBinding,
        node: &NodeEntry,
    ) -> Result<Value, String> {
        let command = NodeControl::Processes {
            cluster_id: binding.bootstrap.cluster_id.clone(),
            node_id: node.id,
        };
        let result = if node.id == binding.node_id {
            crowdb_console_shared::deployment::admission::local_control(
                &crate::node::control_socket(),
                &command,
            )
            .await
        } else {
            let mut target = node.clone();
            target.ssh_key = Some(crate::node::node_key_path(state).to_string_lossy().into_owned());
            crowdb_console_shared::deployment::admission::remote_control(&target, &command).await
        };
        result.map_err(|error| error.to_string())
    }
    async fn health(
        &self,
        state: &AppState,
        config: &ConsoleConfig,
        entry: &ServerEntry,
        pid: Option<u32>,
        intents: &[NodeServiceIntent],
    ) -> NodeHealth {
        let snapshot = state.monitor_cache.snapshot().await;
        super::observe_health(state, config, entry, pid, &snapshot, intents).await
    }
}
