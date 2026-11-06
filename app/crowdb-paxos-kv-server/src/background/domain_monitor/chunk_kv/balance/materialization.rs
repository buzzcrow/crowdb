// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Release completed handoff markers after current-owner recovery.

use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, TransferPhase, TransferTransition,
};

use crate::group0_control_plane::Group0ControlPlane;

use super::{catalog, partition_load, read_transfers, PlanningState};

pub(super) async fn reconcile(
    control: &Group0ControlPlane,
    catalog: &catalog::VersionedRangeCatalog,
    state: &PlanningState,
) -> Result<bool, String> {
    let candidates: Vec<_> = catalog
        .pages
        .iter()
        .flat_map(|page| page.entries.iter())
        .filter(|entry| independently_recovered(entry, state))
        .collect();
    if candidates.is_empty() {
        return Ok(false);
    }
    let transfers = read_transfers(control).await?;
    for entry in candidates {
        if transfers
            .iter()
            .any(|(transition, _)| committed_target(entry, transition))
        {
            catalog::publish_materialized_transfer(control, entry).await?;
            return Ok(true);
        }
        if entry.artifact.tail_overlay.is_some() {
            catalog::publish_materialized_partition(control, entry.partition_id).await?;
            return Ok(true);
        }
    }
    Ok(false)
}

fn independently_recovered(entry: &ChunkKvRangeCatalogEntry, state: &PlanningState) -> bool {
    entry.state == ChunkKvRangeCatalogPartitionState::Serving
        && (entry.transition_id.is_some() || entry.artifact.tail_overlay.is_some())
        && state
            .hosted_epochs
            .get(&(entry.owner.instance_id, entry.partition_id))
            == Some(&entry.owner_epoch)
        && partition_load(state, entry).is_some_and(|load| load.independently_recoverable)
}

fn committed_target(entry: &ChunkKvRangeCatalogEntry, transition: &TransferTransition) -> bool {
    transition.phase == TransferPhase::CatalogCommitted
        && entry.transition_id == Some(transition.transition_id)
        && entry.partition_id == transition.partition_id
        && entry.range == transition.range
        && entry.owner.instance_id == transition.target.instance_id
        && entry.owner_epoch == transition.target_epoch
        && entry.artifact == transition.target_artifact
}
