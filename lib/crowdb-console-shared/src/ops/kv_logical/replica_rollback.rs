// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Undo local replica creation without hiding incomplete cleanup.

use crate::error::{Error, Result};
use crate::ops::OpContext;

use super::{already_absent, server_client};

pub(super) struct ReplicaRollback<'a> {
    pub ctx: &'a OpContext,
    pub store_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    pub target_node: u64,
    pub remove_store: bool,
    pub wired_peers: Vec<u64>,
}

impl ReplicaRollback<'_> {
    pub async fn fail(&self, original: Error) -> Error {
        let mut failures = Vec::new();
        for node_id in &self.wired_peers {
            collect_failure(&mut failures, self.remove_remote(*node_id).await);
        }
        collect_failure(&mut failures, self.remove_target().await);
        if failures.is_empty() {
            original
        } else {
            Error::UpstreamRpc {
                node_id: self.target_node.to_string(),
                status: format!("{original}; rollback incomplete: {}", failures.join("; ")),
            }
        }
    }

    async fn remove_remote(&self, node_id: u64) -> Result<()> {
        server_client(self.ctx, node_id)
            .await?
            .remove_remote_replica(self.store_id, self.group_id, self.replica_id)
            .await
    }

    async fn remove_target(&self) -> Result<()> {
        let client = server_client(self.ctx, self.target_node).await?;
        if self.remove_store {
            client.remove_store(self.store_id).await
        } else {
            client.remove_group(self.store_id, self.group_id).await
        }
    }
}

fn collect_failure(failures: &mut Vec<String>, result: Result<()>) {
    if let Err(error) = result {
        if !already_absent(&error) {
            failures.push(error.to_string());
        }
    }
}
