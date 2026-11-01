// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fence observations across accepted, failed, and cancelled lifecycle work.

use super::cache::{Authority, ProcessFact, ServiceKey, ServiceObservations};
use crate::state::AppState;
use crowdb_console_shared::config::ServiceType;
use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceIntent};
use std::sync::Arc;
use tokio::time::Instant;

impl ServiceObservations {
    pub fn mutation(state: &AppState, node: u64, service: &str) -> Mutation {
        let requested = service.to_owned();
        let config = state.config.read().unwrap();
        let service = config
            .servers
            .iter()
            .find(|entry| {
                entry.node_id == Some(node)
                    && (entry.id == service
                        || (entry.service_type == ServiceType::PaxosKv && service == format!("kv-{node}")))
            })
            .map_or_else(|| service.to_owned(), |entry| entry.id.clone());
        drop(config);
        let key = (node, service);
        let binding = crate::node::cluster_binding(state).ok().flatten();
        state.service_observations.synchronize(binding.as_ref());
        let mut epoch = 0;
        let mut previous_operation = None;
        state.service_observations.latest.rcu(|current| {
            previous_operation = current.authority.as_ref().and_then(|authority| {
                authority
                    .intents
                    .iter()
                    .find(|intent| intent.node_id == node && intent.service_id == requested)
                    .map(|intent| intent.operation_id.clone())
            });
            let mut next = (**current).clone();
            *next.epochs.entry(key.clone()).or_default() += 1;
            epoch = next.epoch(node, &key.1);
            next.authority_epoch += 1;
            next.processes.remove(&key);
            next.health.remove(&key);
            Arc::new(next)
        });
        Mutation {
            observations: Arc::clone(&state.service_observations),
            key,
            epoch,
            binding,
            previous_operation,
            started: Instant::now(),
        }
    }
}

/// Cancellation and unknown outcomes fence in-flight reads without replaying mutations.
pub(crate) struct Mutation {
    observations: Arc<ServiceObservations>,
    key: ServiceKey,
    epoch: u64,
    binding: Option<NodeBinding>,
    previous_operation: Option<String>,
    started: Instant,
}

impl Mutation {
    pub(crate) fn complete(
        &self,
        state: &AppState,
        mut intent: NodeServiceIntent,
        result: &serde_json::Value,
    ) {
        let Some(operation) = result["operation_id"].as_str() else {
            return;
        };
        operation.clone_into(&mut intent.operation_id);
        let Some(binding) = self
            .binding
            .as_ref()
            .filter(|binding| crate::node::cluster_binding(state).ok().flatten().as_ref() == Some(*binding))
        else {
            return;
        };
        let config = state.config.read().unwrap().clone();
        let Some(node) = config.node(self.key.0) else {
            return;
        };
        let mut accepted = false;
        self.observations.latest.rcu(|current| {
            accepted = false;
            if current.binding.as_ref() != Some(binding)
                || current.epoch(self.key.0, &self.key.1) != self.epoch
            {
                return Arc::clone(current);
            }
            let known = current.authority.as_ref().and_then(|authority| {
                authority
                    .intents
                    .iter()
                    .find(|known| known.service_id == intent.service_id && known.node_id == intent.node_id)
            });
            if known.is_some_and(|known| {
                known.operation_id != intent.operation_id
                    && Some(&known.operation_id) != self.previous_operation.as_ref()
            }) {
                return Arc::clone(current);
            }
            let mut next = (**current).clone();
            next.authority_epoch += 1;
            *next.epochs.entry(self.key.clone()).or_default() += 1;
            let authority = next.authority.get_or_insert_with(|| Authority {
                started: self.started,
                intents: Vec::new(),
            });
            authority
                .intents
                .retain(|known| known.service_id != intent.service_id);
            authority.intents.push(intent.clone());
            let entry = crate::services::remote::project(&intent).or_else(|| {
                config
                    .servers
                    .iter()
                    .find(|entry| entry.id == self.key.1)
                    .cloned()
            });
            if let Some(entry) = entry {
                let sample = next.sample(binding, node, &entry);
                let pid = result["pid"]
                    .as_u64()
                    .and_then(|pid| u32::try_from(pid).ok())
                    .filter(|pid| *pid > 0);
                next.processes.insert(
                    self.key.clone(),
                    ProcessFact {
                        sample,
                        started: self.started,
                        pid,
                        exhausted: false,
                    },
                );
            }
            next.health.remove(&self.key);
            accepted = true;
            Arc::new(next)
        });
        if accepted {
            crate::services::remote::project_intents(state, &[intent]);
        }
    }
}

impl Drop for Mutation {
    fn drop(&mut self) {
        self.observations.latest.rcu(|current| {
            if current.binding != self.binding || current.epoch(self.key.0, &self.key.1) != self.epoch {
                return Arc::clone(current);
            }
            let mut next = (**current).clone();
            *next.epochs.entry(self.key.clone()).or_default() += 1;
            next.authority_epoch += 1;
            next.processes.remove(&self.key);
            next.health.remove(&self.key);
            Arc::new(next)
        });
        self.observations.changed.notify_waiters();
    }
}
