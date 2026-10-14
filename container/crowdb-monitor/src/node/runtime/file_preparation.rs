// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One background file allocator; private control never waits for preallocation.

use std::path::Path;

use crowdb_protocol::mgmt::node::{
    storage::{FilePreparation, FilePreparationPhase, PrepareDiskFile},
    NodeBinding,
};

use super::{durable, Result};
use crate::node::storage::file_preparation as files;

#[derive(Default)]
pub(super) struct FileAllocator {
    task: Option<tokio::task::JoinHandle<()>>,
    operation_id: Option<String>,
}

impl FileAllocator {
    pub(super) fn running(&self) -> bool {
        self.task.as_ref().is_some_and(|task| !task.is_finished())
    }

    pub(super) async fn accept(&mut self, root: &Path, request: PrepareDiskFile) -> Result<FilePreparation> {
        authorize(root, &request).await?;
        if self.running() {
            return if self.operation_id.as_ref() == Some(&request.operation_id) {
                Ok(files::accept(root, &request)?)
            } else {
                Err("another file preparation is still running".into())
            };
        }
        files::accept(root, &request)?;
        durable::write(&root.join("active-disk-file.json"), &request.operation_id)?;
        let status = files::pending(root, &request.operation_id)?;
        self.start(root, request.operation_id);
        Ok(status)
    }

    pub(super) fn resume(&mut self, root: &Path) -> Result<()> {
        if let Some(id) = durable::read::<String>(&root.join("active-disk-file.json"))? {
            let status = files::status(root, &id)?;
            if matches!(
                status.phase,
                FilePreparationPhase::Accepted | FilePreparationPhase::Creating
            ) {
                self.start(root, id);
            }
        }
        Ok(())
    }

    fn start(&mut self, root: &Path, id: String) {
        self.operation_id = Some(id.clone());
        let root = root.to_path_buf();
        self.task = Some(tokio::spawn(async move {
            let result: Result<()> = async {
                let status = files::status(&root, &id)?;
                authorize(&root, &status.request).await?;
                let execution_root = root.clone();
                let execution_id = id.clone();
                let state =
                    tokio::task::spawn_blocking(move || files::execute(&execution_root, &execution_id))
                        .await??;
                if let Some(error) = state.error {
                    return Err(error.into());
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                eprintln!("disk file preparation failed; explicit retry required: {error}");
                if let Err(persistence) = files::failed(&root, &id, error.to_string()) {
                    eprintln!("cannot persist disk file preparation failure: {persistence}");
                }
            }
        }));
    }

    pub(super) async fn stop(&mut self) {
        // A blocking fallocate cannot safely be aborted. Keep its ownership
        // journal alive until it completes, including during graceful shutdown.
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

async fn authorize(root: &Path, request: &PrepareDiskFile) -> Result<()> {
    let binding: NodeBinding = durable::read(&root.join("node-binding.json"))?
        .ok_or("file creation is waiting for Group 0 binding")?;
    if binding.bootstrap.cluster_id != request.cluster_id || binding.node_id != request.node_id {
        return Err("file preparation belongs to another node or cluster".into());
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), super::verify_binding(&binding)).await??;
    Ok(())
}
