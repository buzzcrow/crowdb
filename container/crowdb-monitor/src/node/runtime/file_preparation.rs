// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded file workers; private control never waits for preallocation or `DiskIO`.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::Path,
};

use crowdb_protocol::mgmt::node::{
    storage::{FilePreparation, FilePreparationPhase, PrepareDiskFile},
    NodeBinding,
};

use super::{durable, Result};
use crate::node::storage::file_preparation as files;

#[derive(Default)]
pub(super) struct FileAllocator {
    tasks: BTreeMap<String, tokio::task::JoinHandle<()>>,
    queued: VecDeque<String>,
    known: BTreeSet<String>,
}

impl FileAllocator {
    pub(super) fn running(&self) -> bool {
        self.tasks.values().any(|task| !task.is_finished())
    }

    pub(super) async fn accept(&mut self, root: &Path, request: PrepareDiskFile) -> Result<FilePreparation> {
        authorize(root, &request).await?;
        if self.known.contains(&request.operation_id) {
            return Ok(files::accept(root, &request)?);
        }
        if self.known.len() >= 4096 {
            return Err("file preparation queue exceeds bound".into());
        }
        files::accept(root, &request)?;
        let status = files::pending(root, &request.operation_id)?;
        self.enqueue(request.operation_id);
        self.tick(root)?;
        Ok(status)
    }

    pub(super) fn resume(&mut self, root: &Path) -> Result<()> {
        let mut after = None;
        loop {
            let page = crate::node::storage::file_operations::list(root, after.as_deref(), 32)?;
            for state in page.items {
                if resumable(&state) || state.phase == FilePreparationPhase::Unknown {
                    self.enqueue(state.request.operation_id);
                }
            }
            after = page.next;
            if after.is_none() {
                break;
            }
        }
        self.tick(root)?;
        Ok(())
    }

    fn enqueue(&mut self, id: String) {
        if self.known.insert(id.clone()) {
            self.queued.push_back(id);
        }
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
            if resumable(&files::status(root, &id)?) {
                self.queued.push_back(id);
            } else {
                self.known.remove(&id);
            }
        }
        while self.tasks.len() < 4 {
            let Some(id) = self.queued.pop_front() else {
                break;
            };
            self.start(root, id);
        }
        Ok(())
    }

    fn start(&mut self, root: &Path, id: String) {
        let task_id = id.clone();
        let root = root.to_path_buf();
        let task = tokio::spawn(async move {
            let result: Result<()> = async {
                let status = files::status(&root, &id)?;
                authorize(&root, &status.request).await?;
                let execution_root = root.clone();
                let execution_id = id.clone();
                let state =
                    tokio::task::spawn_blocking(move || files::execute(&execution_root, &execution_id))
                        .await??;
                if let Some(error) = &state.error {
                    return Err(error.clone().into());
                }
                super::file_admission::publish(&root, &state).await?;
                Ok(())
            }
            .await;
            if let Err(error) = result {
                eprintln!("disk file preparation failed; explicit retry required: {error}");
                let current = files::status(&root, &id).ok().map(|state| state.phase);
                let phase = if current == Some(FilePreparationPhase::WaitingForDiskIO)
                    && root.join("node-binding.json").exists()
                {
                    FilePreparationPhase::WaitingForDiskIO
                } else if current == Some(FilePreparationPhase::Unknown)
                    || matches!(
                        error.downcast_ref::<crowdb_paxos_kv_client::Error>(),
                        Some(crowdb_paxos_kv_client::Error::OutcomeUnknown)
                    )
                {
                    FilePreparationPhase::Unknown
                } else {
                    FilePreparationPhase::Failed
                };
                if let Err(persistence) = files::outcome(&root, &id, phase, Some(error.to_string())) {
                    eprintln!("cannot persist disk file preparation failure: {persistence}");
                }
            }
        });
        self.tasks.insert(task_id, task);
    }

    pub(super) async fn stop(&mut self) {
        // A blocking fallocate cannot safely be aborted. Keep its ownership
        // journal alive until it completes, including during graceful shutdown.
        for (_, task) in std::mem::take(&mut self.tasks) {
            let _ = task.await;
        }
    }
}

fn resumable(state: &FilePreparation) -> bool {
    (state.phase == FilePreparationPhase::Prepared && state.request.admission.is_some())
        || matches!(
            state.phase,
            FilePreparationPhase::Accepted
                | FilePreparationPhase::Creating
                | FilePreparationPhase::Publishing
                | FilePreparationPhase::WaitingForDiskIO
        )
}

async fn authorize(root: &Path, request: &PrepareDiskFile) -> Result<()> {
    let binding: NodeBinding = durable::read(&root.join("node-binding.json"))?
        .ok_or("file creation is waiting for Group 0 binding")?;
    if binding.bootstrap.cluster_id != request.cluster_id || binding.node_id != request.node_id {
        return Err("file preparation belongs to another node or cluster".into());
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), super::verify_binding(&binding)).await??;
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        super::file_admission::validate_placement(root, request),
    )
    .await??;
    Ok(())
}

#[cfg(feature = "test-util")]
/// Resume durable file work through the production queue and wait for one pass.
///
/// # Errors
/// Propagates durable queue discovery failures.
pub async fn resume_file_operations_for_tests(root: &Path) -> Result<()> {
    let mut allocator = FileAllocator::default();
    allocator.resume(root)?;
    allocator.stop().await;
    Ok(())
}
