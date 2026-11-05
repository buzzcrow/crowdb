// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Preparation can stage private packs while the published source still writes.

use super::{same_recovery_base, KvRootCatalogStore};
use crate::Group0ControlStore;
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogEntry, TransferPhase};
use crowdb_tree_ffi::CtError;
use std::sync::atomic::Ordering;

impl KvRootCatalogStore {
    pub(super) async fn check_prepared_staging(&self, authority_epoch: u64) -> Result<(), CtError> {
        self.check_process_owner()?;
        let prepared = self.prepared_assignment.as_ref().ok_or(CtError::Unavailable)?;
        let identity = self
            .process
            .as_ref()
            .ok_or(CtError::Unavailable)?
            .status(std::time::Instant::now())
            .lease
            .identity;
        let id = prepared.transition_id.ok_or(CtError::Unavailable)?;
        let control = Group0ControlStore::from_client(self.kv.clone());
        let (transition, _) = control
            .load_transfer_transition(id)
            .await
            .map_err(|_| CtError::Unavailable)?
            .ok_or(CtError::Unavailable)?;
        let allowed = matches!(
            transition.phase,
            TransferPhase::TargetPreparing
                | TransferPhase::TargetPrepared
                | TransferPhase::AwaitingFence
                | TransferPhase::TargetCatchingUp
                | TransferPhase::CatchupPublished
                | TransferPhase::TargetReady
        );
        let current = ChunkKvRangeCatalogEntry {
            partition_id: transition.partition_id,
            range: transition.range,
            owner: transition.target,
            owner_epoch: transition.target_epoch,
            state: prepared.state,
            artifact: transition.target_artifact,
            transition_id: Some(transition.transition_id),
        };
        if !allowed
            || identity.instance_id != current.owner.instance_id
            // A retained split keeps its Tree writer as partition epochs advance.
            || (authority_epoch != self.owner_epoch
                && (authority_epoch == 0 || authority_epoch > transition.source_epoch))
            || current.partition_id != prepared.partition_id
            || current.range != prepared.range
            || current.owner != prepared.owner
            || current.owner_epoch != self.owner_epoch
            || current.artifact.tree_id != self.tree_id
            || current.artifact.stream_name != prepared.artifact.stream_name
            || !same_recovery_base(&current, prepared)
        {
            tracing::warn!(tree_id = self.tree_id, phase = ?transition.phase, authority_epoch,
                owner_epoch = self.owner_epoch, process_owner = identity.instance_id,
                target_owner = current.owner.instance_id, same_base = same_recovery_base(&current, prepared),
                "prepared tree staging no longer matches its transfer intent");
            return Err(CtError::Unavailable);
        }
        Ok(())
    }

    pub(super) async fn authorize_staging(&self) -> Result<(), CtError> {
        if self.publication_authorized.load(Ordering::Acquire) {
            return self.authorize_publication().await;
        }
        // Only the existing exact published-owner proof may advance root epoch.
        // Otherwise a current durable preparation intent permits private packs.
        if self.claim_published(false).await.is_ok() {
            self.register_writer().await?;
            return self.persist_completed_recovery().await;
        }
        let authority = self.protection_authority(false).await?;
        self.check_prepared_staging(authority.epoch).await?;
        self.register_writer().await
    }
}
