// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact local handles for catalog assignments.

use crowdb_chunk_kv::{Partition, PartitionLifecycle};
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState};

pub(super) fn partition_matches_entry(partition: &Partition, entry: &ChunkKvRangeCatalogEntry) -> bool {
    let snapshot = partition.snapshot();
    // Materialization retires the overlay commit proof. A writer still waiting
    // on that proof must reopen the independently checkpointed assignment.
    let needs_independent_recovery = entry.artifact.tail_overlay.is_none()
        && entry.state == ChunkKvRangeCatalogPartitionState::Serving
        && snapshot.lifecycle == PartitionLifecycle::Prepared
        && partition.is_prepared_split_child();
    !needs_independent_recovery
        && partition.tree_id() == entry.artifact.tree_id
        && snapshot.partition_id.high == entry.partition_id.high
        && snapshot.partition_id.low == entry.partition_id.low
        && snapshot.ownership_epoch == entry.owner_epoch
        && snapshot.range.start.as_deref() == Some(entry.range.start.as_slice())
        && snapshot.range.end == entry.range.end
        && snapshot.stream_name == entry.artifact.stream_name
}

pub(super) fn local_partition_for_entry(
    partition: &Partition,
    entry: &ChunkKvRangeCatalogEntry,
) -> Option<Partition> {
    if partition_matches_entry(partition, entry) {
        return Some(partition.clone());
    }
    let ingress = partition.split_ingress()?;
    [ingress.retained_parent(), ingress.child()]
        .into_iter()
        .find(|writer| partition_matches_entry(writer, entry))
}
