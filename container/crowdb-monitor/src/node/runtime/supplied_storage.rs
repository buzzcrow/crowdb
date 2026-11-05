// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded monitor-owned workers for explicitly selected external media.

use super::{durable, Result};
use crate::node::storage::supplied as journal;
use crowdb_protocol::mgmt::node::{
    storage::{PrepareSuppliedDisk, SuppliedPreparation, SuppliedPreparationPhase},
    NodeBinding,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::Path,
    time::Duration,
};

mod execution;

#[derive(Default)]
pub(super) struct SuppliedStorage {
    tasks: BTreeMap<String, tokio::task::JoinHandle<()>>,
    queued: VecDeque<String>,
    known: BTreeSet<String>,
}

impl SuppliedStorage {
    pub(super) fn retire(
        root: &Path,
        bootstrap: &crowdb_protocol::mgmt::SystemBootstrapIdentity,
        confirmed: bool,
    ) -> Result<()> {
        if journal::operations(root)?.is_empty() {
            return Ok(());
        }
        let binding: NodeBinding = durable::read(&root.join("node-binding.json"))?
            .ok_or("supplied cleanup requires retained node binding")?;
        if !confirmed || binding.bootstrap != *bootstrap {
            return Err("supplied cleanup differs from accepted bootstrap".into());
        }
        durable::write(&retirement(root, &binding), bootstrap)?;
        Ok(())
    }
    pub(super) fn running(&self) -> bool {
        self.tasks.values().any(|task| !task.is_finished())
    }

    pub(super) async fn accept(
        &mut self,
        root: &Path,
        request: PrepareSuppliedDisk,
    ) -> Result<SuppliedPreparation> {
        authorize(root, &request).await?;
        self.tick(root)?;
        let id = request.record.operation_id.clone();
        if self.known.contains(&id) {
            let current = journal::status(root, &id)?;
            if current.request != request {
                return Err("supplied operation inputs changed".into());
            }
            return Ok(current);
        }
        if self.known.len() >= 4096 {
            return Err("supplied queue exceeds bound".into());
        }
        let state = journal::accept(root, request)?;
        if state.phase != SuppliedPreparationPhase::Registered {
            self.enqueue(id);
        }
        self.tick(root)?;
        Ok(state)
    }

    fn enqueue(&mut self, id: String) {
        if self.known.insert(id.clone()) {
            self.queued.push_back(id);
        }
    }

    pub(super) fn resume(&mut self, root: &Path) -> Result<()> {
        for id in journal::operations(root)? {
            if resumable(journal::status(root, &id)?.phase) {
                self.enqueue(id);
            }
        }
        self.tick(root)
    }

    pub(super) fn tick(&mut self, root: &Path) -> Result<()> {
        let finished: Vec<_> = self
            .tasks
            .iter()
            .filter(|(_, task)| task.is_finished())
            .map(|(id, _)| id.clone())
            .collect();
        for id in finished {
            self.tasks.remove(&id);
            if resumable(journal::status(root, &id)?.phase) {
                self.queued.push_back(id);
            } else {
                self.known.remove(&id);
            }
        }
        while self.tasks.len() < 4 {
            let Some(id) = self.queued.pop_front() else { break };
            let root = root.to_path_buf();
            let task_id = id.clone();
            self.tasks.insert(
                task_id,
                tokio::spawn(async move {
                    if let Err(error) = execution::run(&root, &id).await {
                        eprintln!("supplied disk provisioning failed; explicit retry required: {error}");
                        if let Ok(mut state) = journal::status(&root, &id) {
                            state.phase = if matches!(
                                error.downcast_ref::<crowdb_paxos_kv_client::Error>(),
                                Some(crowdb_paxos_kv_client::Error::OutcomeUnknown)
                            ) {
                                SuppliedPreparationPhase::Unknown
                            } else {
                                SuppliedPreparationPhase::Failed
                            };
                            state.error = Some(error.to_string().chars().take(128).collect());
                            if let Err(error) = journal::save(&root, &state) {
                                eprintln!("cannot persist supplied disk failure: {error}");
                            }
                        }
                    }
                }),
            );
        }
        Ok(())
    }

    pub(super) async fn stop(&mut self) {
        for (_, task) in std::mem::take(&mut self.tasks) {
            let _ = task.await;
        }
    }
}

fn resumable(phase: SuppliedPreparationPhase) -> bool {
    !matches!(
        phase,
        SuppliedPreparationPhase::Registered | SuppliedPreparationPhase::Failed
    )
}

async fn authorize(root: &Path, request: &PrepareSuppliedDisk) -> Result<NodeBinding> {
    request.validate()?;
    let binding: NodeBinding = durable::read(&root.join("node-binding.json"))?
        .ok_or("supplied disk is waiting for confirmed node binding")?;
    if retirement(root, &binding).exists()
        || binding.node_id != request.record.node_id
        || binding.bootstrap.cluster_id != request.cluster_id
        || root
            .join(format!(
                "retired-bootstrap-{}.json",
                binding.bootstrap.operation_id
            ))
            .exists()
    {
        return Err("supplied disk belongs to another or retired node admission".into());
    }
    tokio::time::timeout(Duration::from_secs(3), super::verify_binding(&binding)).await??;
    Ok(binding)
}

fn retirement(root: &Path, binding: &NodeBinding) -> std::path::PathBuf {
    root.join(format!(
        "supplied-storage-retired-{}.json",
        binding.bootstrap.operation_id
    ))
}

#[cfg(feature = "test-util")]
/// # Errors
/// Propagates production authorization, immutable journal and provisioning failures.
pub async fn prepare_supplied_disk_for_tests(
    root: &Path,
    request: PrepareSuppliedDisk,
    inputs: Vec<crowdb_protocol::mgmt::node::storage::DiskInput>,
) -> Result<SuppliedPreparation> {
    authorize(root, &request).await?;
    let id = request.record.operation_id.clone();
    journal::accept_with_inputs(root, request, &inputs)?;
    execution::run_with_inputs(root, &id, inputs).await?;
    Ok(journal::status(root, &id)?)
}
