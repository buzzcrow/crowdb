// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Identity-fenced facts sampled independently of inventory readers.

use arc_swap::ArcSwap;
use crowdb_console_shared::{
    cluster::NodeHealth,
    config::{NodeEntry, ServerEntry, ServiceType},
};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceAction, NodeServiceIntent};
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

pub(super) const MAX_AGE: Duration = Duration::from_secs(3);
pub(super) type ServiceKey = (u64, String);

#[derive(Default)]
pub(crate) struct ServiceObservations {
    pub(crate) latest: ArcSwap<Inventory>,
    pub(super) changed: Notify,
    pub(super) running: AtomicBool,
}

#[derive(Clone, Default)]
pub(crate) struct Inventory {
    pub binding: Option<NodeBinding>,
    pub authority_epoch: u64,
    pub authority: Option<Authority>,
    pub(super) epochs: BTreeMap<ServiceKey, u64>,
    pub(super) processes: BTreeMap<ServiceKey, ProcessFact>,
    pub(super) health: BTreeMap<ServiceKey, HealthFact>,
}

#[derive(Clone)]
pub(crate) struct Authority {
    pub started: Instant,
    pub intents: Vec<NodeServiceIntent>,
}

#[derive(Clone)]
pub(super) struct Sample {
    pub binding: NodeBinding,
    pub node: NodeEntry,
    pub entry: ServerEntry,
    pub intent: Option<NodeServiceIntent>,
    pub epoch: u64,
}

#[derive(Clone)]
pub(super) struct ProcessFact {
    pub(super) sample: Sample,
    pub(super) started: Instant,
    pub(super) pid: Option<u32>,
    pub(super) exhausted: bool,
    pub(super) observed: bool,
}

#[derive(Clone)]
pub(super) struct HealthFact {
    sample: Sample,
    started: Instant,
    pub(super) health: NodeHealth,
}

impl Inventory {
    pub fn intent(&self, entry: &ServerEntry) -> Option<&NodeServiceIntent> {
        self.authority.as_ref()?.intents.iter().find(|intent| {
            Some(intent.node_id) == entry.node_id
                && (intent.service_id == entry.id
                    || (entry.service_type == ServiceType::PaxosKv && intent.kind == "kv"))
        })
    }

    pub(super) fn sample(&self, binding: &NodeBinding, node: &NodeEntry, entry: &ServerEntry) -> Sample {
        Sample {
            binding: binding.clone(),
            node: node.clone(),
            entry: entry.clone(),
            intent: self.intent(entry).cloned(),
            epoch: self.epoch(node.id, &entry.id),
        }
    }

    pub(super) fn epoch(&self, node: u64, service: &str) -> u64 {
        self.epochs.get(&(node, service.to_owned())).copied().unwrap_or(0)
    }

    fn accepts(&self, sample: &Sample) -> bool {
        self.binding.as_ref() == Some(&sample.binding)
            && self.epoch(sample.node.id, &sample.entry.id) == sample.epoch
            && self.intent(&sample.entry) == sample.intent.as_ref()
    }

    pub fn read(
        &self,
        binding: Option<&NodeBinding>,
        node: Option<&NodeEntry>,
        entry: &ServerEntry,
    ) -> (Option<u32>, NodeHealth, bool, bool) {
        let Some(node) = node.filter(|_| self.binding.as_ref() == binding && binding.is_some()) else {
            return (None, NodeHealth::Unknown, false, false);
        };
        let key = (node.id, entry.id.clone());
        let valid = |sample: &Sample| self.accepts(sample) && sample.node == *node && sample.entry == *entry;
        let process = self
            .processes
            .get(&key)
            .filter(|fact| valid(&fact.sample) && fact.started.elapsed() <= MAX_AGE);
        let pid = process.and_then(|fact| fact.pid);
        let fresh_intent = self
            .authority
            .as_ref()
            .is_some_and(|authority| authority.started.elapsed() <= MAX_AGE);
        let health = self
            .health
            .get(&key)
            .filter(|fact| valid(&fact.sample) && fact.started.elapsed() <= MAX_AGE)
            .filter(|_| entry.service_type != ServiceType::AccessServer || fresh_intent)
            .map_or(NodeHealth::Unknown, |fact| fact.health);
        let pending = fresh_intent
            && pid.is_none()
            && self.intent(entry).is_some_and(|intent| {
                matches!(
                    intent.action,
                    NodeServiceAction::Start | NodeServiceAction::Restart
                )
            })
            && !process.is_some_and(|fact| fact.exhausted);
        (pid, health, pending, process.is_some_and(|fact| fact.observed))
    }
}

impl ServiceObservations {
    pub fn synchronize(&self, binding: Option<&NodeBinding>) {
        self.latest.rcu(|current| {
            if current.binding.as_ref() == binding {
                return Arc::clone(current);
            }
            Arc::new(Inventory {
                binding: binding.cloned(),
                authority_epoch: current.authority_epoch + 1,
                ..Inventory::default()
            })
        });
    }

    pub fn authority(
        &self,
        binding: &NodeBinding,
        epoch: u64,
        started: Instant,
        intents: &[NodeServiceIntent],
    ) -> bool {
        let mut accepted = false;
        let mut changed = false;
        self.latest.rcu(|current| {
            accepted = current.binding.as_ref() == Some(binding)
                && current.authority_epoch == epoch
                && current
                    .authority
                    .as_ref()
                    .map_or(true, |authority| started >= authority.started);
            if !accepted {
                changed = false;
                return Arc::clone(current);
            }
            changed = current
                .authority
                .as_ref()
                .map_or(true, |authority| authority.intents != intents);
            let mut next = (**current).clone();
            next.authority = Some(Authority {
                started,
                intents: intents.to_vec(),
            });
            Arc::new(next)
        });
        if changed {
            self.changed.notify_waiters();
        }
        accepted
    }

    pub(super) fn process(
        &self,
        sample: &Sample,
        started: Instant,
        pid: Option<u32>,
        exhausted: bool,
        observed: bool,
    ) {
        self.latest.rcu(|current| {
            if !current.accepts(sample) {
                return Arc::clone(current);
            }
            let mut next = (**current).clone();
            next.processes.insert(
                (sample.node.id, sample.entry.id.clone()),
                ProcessFact {
                    sample: sample.clone(),
                    started,
                    pid,
                    exhausted,
                    observed,
                },
            );
            Arc::new(next)
        });
    }

    pub(super) fn health(&self, sample: &Sample, started: Instant, health: NodeHealth) {
        self.latest.rcu(|current| {
            if !current.accepts(sample) {
                return Arc::clone(current);
            }
            let mut next = (**current).clone();
            next.health.insert(
                (sample.node.id, sample.entry.id.clone()),
                HealthFact {
                    sample: sample.clone(),
                    started,
                    health,
                },
            );
            Arc::new(next)
        });
    }
}
