// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Typed outcomes for replaying exact persisted block incarnations.

use super::DiskdbClientPool;
use crowdb_diskdb_client::DiskdbClientError;
use crowdb_protocol::diskdb::rpc::{
    CommitBlocksRequest, FreeBlocksRequest, FreeFailureReason, FreeResponse, Segment,
};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub(crate) enum BlockMutationError {
    #[error("invalid persisted segment: {0}")]
    InvalidSegment(String),
    #[error("block owner discovery pending: {0}")]
    Routing(String),
    #[error("block mutation RPC: {0}")]
    Rpc(#[from] DiskdbClientError),
}

impl BlockMutationError {
    pub(crate) fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Routing(_)
                | Self::Rpc(
                    DiskdbClientError::ConnectFailed(_)
                        | DiskdbClientError::Unreachable(_)
                        | DiskdbClientError::NotOwner(_)
                )
        )
    }
}

impl DiskdbClientPool {
    pub(crate) async fn free_blocks_for_recovery(
        &self,
        segments: Vec<Segment>,
    ) -> Result<(), BlockMutationError> {
        self.mutate_blocks(segments, false).await
    }

    pub(crate) async fn commit_blocks_for_recovery(
        &self,
        segments: Vec<Segment>,
    ) -> Result<(), BlockMutationError> {
        self.mutate_blocks(segments, true).await
    }

    async fn mutate_blocks(&self, segments: Vec<Segment>, commit: bool) -> Result<(), BlockMutationError> {
        let mut grouped = HashMap::<u64, Vec<Segment>>::new();
        for segment in segments {
            let disk = segment
                .disk_id
                .ok_or_else(|| BlockMutationError::InvalidSegment("missing disk ID".into()))?;
            let group = self.dg_for_disk(&disk).ok_or_else(|| {
                BlockMutationError::Routing(format!("disk {disk:?} has no disk-group mapping"))
            })?;
            grouped.entry(group).or_default().push(segment);
        }
        let mut work = Vec::with_capacity(grouped.len());
        for (group, segments) in grouped {
            let endpoint = self
                .endpoint_for_dg(group)
                .await
                .map_err(BlockMutationError::Routing)?;
            work.push(async move {
                let result = if commit {
                    self.transport
                        .commit_blocks(&endpoint, &CommitBlocksRequest { segments })
                        .await
                        .map(|_| ())
                } else {
                    self.transport
                        .free_blocks(&endpoint, &FreeBlocksRequest { segments })
                        .await
                        .and_then(|response| validate_free(&response))
                };
                self.refresh_after_owner_failure(result)
                    .await
                    .map_err(BlockMutationError::Rpc)
            });
        }
        // Await every submitted mutation before inspecting outcomes. A terminal
        // error must not be hidden by an earlier transient owner failure.
        let mut pending = None;
        for result in futures::future::join_all(work).await {
            if let Err(error) = result {
                if !error.is_retryable() {
                    return Err(error);
                }
                pending = Some(error);
            }
        }
        pending.map_or(Ok(()), Err)
    }
}

fn validate_free(response: &FreeResponse) -> Result<(), DiskdbClientError> {
    // A replay may find an already freed or subsequently reused incarnation.
    // Neither permits freeing the current owner, and neither needs more work.
    if response.failures.iter().any(|failure| {
        !matches!(
            failure.reason,
            FreeFailureReason::NotBusy | FreeFailureReason::IncarnationMismatch
        )
    }) {
        return Err(DiskdbClientError::Unreachable(format!(
            "block cleanup remains pending: {:?}",
            response.failures
        )));
    }
    Ok(())
}
