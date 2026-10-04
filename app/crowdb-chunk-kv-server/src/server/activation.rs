// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Activation fenced by exact durable catalog transition evidence.

use super::{ChunkKvError, ChunkKvService};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogPartitionState, Id128, KeyRange, SplitPhase, SplitTransition, TransferPhase,
    TransferTransition,
};

impl ChunkKvService {
    /// Activates a recovered split writer only with exact committed catalog evidence.
    ///
    /// # Errors
    ///
    /// Rejects uncommitted, stale or mismatched transition and assignment fields.
    pub fn activate_recovered_split_partition(
        &self,
        partition_id: Id128,
        owner_epoch: u64,
        transition: &SplitTransition,
    ) -> Result<(), ChunkKvError> {
        transition
            .validate()
            .map_err(|error| ChunkKvError::InvalidRequest(error.to_string()))?;
        let catalog = self.catalog.load();
        let entry = catalog
            .entry_for_partition(partition_id)
            .ok_or(ChunkKvError::OutOfRange)?;
        let (range, owner, epoch, artifact) = if partition_id == transition.parent_id {
            (
                KeyRange {
                    start: transition.parent_range.start.clone(),
                    end: Some(transition.split_key.clone()),
                },
                &transition.parent_owner,
                transition.parent_next_epoch,
                &transition.retained_parent_artifact,
            )
        } else if partition_id == transition.child.partition_id {
            (
                transition.child.range.clone(),
                &transition.child.owner,
                transition.child.owner_epoch,
                &transition.child.artifact,
            )
        } else {
            return Err(ChunkKvError::OutOfRange);
        };
        if transition.phase != SplitPhase::CatalogCommitted
            || entry.transition_id != Some(transition.transition_id)
            || entry.range != range
            || &entry.owner != owner
            || owner.instance_id != self.instance_id
            || epoch != owner_epoch
            || entry.owner_epoch != epoch
            || &entry.artifact != artifact
            || entry.state != ChunkKvRangeCatalogPartitionState::Serving
        {
            return Err(ChunkKvError::NotServing(
                "committed split does not prove this serving assignment".into(),
            ));
        }
        self.partitions
            .load()
            .get(&partition_id)
            .ok_or(ChunkKvError::OutOfRange)?
            .activate_recovered_overlay(owner_epoch)
    }

    /// Activates one replayed assignment after a matching catalog and serving
    /// grant have been installed by the process lifecycle.
    ///
    /// # Errors
    ///
    /// Returns `OutOfRange` when the partition is not hosted, or the precise
    /// partition epoch/lifecycle error when activation is unsafe.
    pub fn activate_recovered_partition(
        &self,
        partition_id: Id128,
        owner_epoch: u64,
    ) -> Result<(), ChunkKvError> {
        let catalog = self.catalog.load();
        let entry = catalog
            .entry_for_partition(partition_id)
            .ok_or(ChunkKvError::OutOfRange)?;
        if entry.owner.instance_id != self.instance_id
            || entry.owner_epoch != owner_epoch
            || entry.state != ChunkKvRangeCatalogPartitionState::Serving
        {
            return Err(ChunkKvError::NotServing(
                "catalog does not publish this serving assignment".into(),
            ));
        }
        // A local split dispatcher owns both replacement writers.  Its old
        // parent identity remains a compatibility route for g1 requests, not
        // a partition that a later grant may reactivate at its obsolete epoch.
        if self.local_split_sessions.load().contains_key(&partition_id) {
            return Ok(());
        }
        self.partitions
            .load()
            .get(&partition_id)
            .ok_or(ChunkKvError::OutOfRange)?
            .activate_recovered(owner_epoch)
    }

    /// Returns the catalog transition attached to one hosted assignment.
    #[must_use]
    pub fn catalog_transition_id(&self, partition_id: Id128) -> Option<Id128> {
        self.catalog
            .load()
            .entry_for_partition(partition_id)
            .and_then(|entry| entry.transition_id)
    }

    /// Activates a recovered overlay only when a committed transfer exactly
    /// proves the current catalog assignment.
    ///
    /// # Errors
    ///
    /// Returns an authority, identity, epoch, or lifecycle error without
    /// activating the target when any durable proof differs.
    pub fn activate_recovered_transfer_partition(
        &self,
        partition_id: Id128,
        owner_epoch: u64,
        transition: &TransferTransition,
    ) -> Result<(), ChunkKvError> {
        transition
            .validate()
            .map_err(|error| ChunkKvError::InvalidRequest(error.to_string()))?;
        let catalog = self.catalog.load();
        let entry = catalog
            .entry_for_partition(partition_id)
            .ok_or(ChunkKvError::OutOfRange)?;
        let exact = transition.phase == TransferPhase::CatalogCommitted
            && entry.transition_id == Some(transition.transition_id)
            && transition.partition_id == partition_id
            && transition.range == entry.range
            && transition.target == entry.owner
            && transition.target.instance_id == self.instance_id
            && transition.target_epoch == owner_epoch
            && transition.target_epoch == entry.owner_epoch
            && transition.target_artifact == entry.artifact
            && entry.state == ChunkKvRangeCatalogPartitionState::Serving;
        if !exact {
            return Err(ChunkKvError::NotServing(
                "committed transfer does not prove this serving assignment".into(),
            ));
        }
        self.partitions
            .load()
            .get(&partition_id)
            .ok_or(ChunkKvError::OutOfRange)?
            .activate_recovered_overlay(owner_epoch)
    }
}
