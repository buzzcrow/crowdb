// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, Id128, KeyRange, OwnerDescriptor, PartitionArtifact, SplitChildAssignment,
    SplitPhase, SplitTransition,
};
use crowdb_protocol::chunk_stream::StreamName;
use sha2::{Digest, Sha256};

pub(super) fn split_transition(
    parent: &ChunkKvRangeCatalogEntry,
    split_key: Vec<u8>,
    now_ms: u64,
) -> Result<SplitTransition, String> {
    let transition_id = derived_id(parent, &split_key, b"transition");
    let parent_next_epoch = parent
        .owner_epoch
        .checked_add(1)
        .ok_or_else(|| "chunk-KV split owner epoch overflowed".to_string())?;
    let child = split_child(
        parent,
        &split_key,
        b"right",
        parent.owner.clone(),
        1,
        KeyRange {
            start: split_key.clone(),
            end: parent.range.end.clone(),
        },
    );
    let transition = SplitTransition {
        transition_id,
        parent_id: parent.partition_id,
        parent_range: parent.range.clone(),
        parent_owner: parent.owner.clone(),
        parent_epoch: parent.owner_epoch,
        parent_artifact: parent.artifact.clone(),
        retained_parent_artifact: split_artifact(parent, &split_key, b"left"),
        parent_next_epoch,
        split_key,
        child,
        planned_at_ms: now_ms,
        phase: SplitPhase::Planned,
        readiness_proof: None,
        failure: None,
    };
    transition.validate().map_err(|error| error.to_string())?;
    Ok(transition)
}

fn split_child(
    parent: &ChunkKvRangeCatalogEntry,
    split_key: &[u8],
    side: &[u8],
    owner: OwnerDescriptor,
    owner_epoch: u64,
    range: KeyRange,
) -> SplitChildAssignment {
    SplitChildAssignment {
        partition_id: derived_id(parent, split_key, &[side, b"-partition"].concat()),
        range,
        owner,
        owner_epoch,
        artifact: split_artifact(parent, split_key, side),
    }
}

fn split_artifact(parent: &ChunkKvRangeCatalogEntry, split_key: &[u8], side: &[u8]) -> PartitionArtifact {
    PartitionArtifact {
        tree_id: derived_u64(parent, split_key, &[side, b"-tree"].concat()),
        stream_name: StreamName {
            high: derived_u64(parent, split_key, &[side, b"-stream-high"].concat()),
            low: derived_u64(parent, split_key, &[side, b"-stream-low"].concat()),
        },
        tail_overlay: None,
    }
}

fn derived_id(parent: &ChunkKvRangeCatalogEntry, split_key: &[u8], label: &[u8]) -> Id128 {
    let digest = split_digest(parent, split_key, label);
    Id128 {
        high: nonzero(u64::from_be_bytes(digest[0..8].try_into().unwrap_or([0; 8]))),
        low: nonzero(u64::from_be_bytes(digest[8..16].try_into().unwrap_or([0; 8]))),
    }
}

fn derived_u64(parent: &ChunkKvRangeCatalogEntry, split_key: &[u8], label: &[u8]) -> u64 {
    let digest = split_digest(parent, split_key, label);
    nonzero(u64::from_be_bytes(digest[0..8].try_into().unwrap_or([0; 8])))
}

fn split_digest(parent: &ChunkKvRangeCatalogEntry, split_key: &[u8], label: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"chunk-kv-split-v1");
    digest.update(label);
    digest.update(parent.partition_id.high.to_be_bytes());
    digest.update(parent.partition_id.low.to_be_bytes());
    digest.update(parent.owner_epoch.to_be_bytes());
    digest.update(split_key);
    digest.finalize().into()
}

fn nonzero(value: u64) -> u64 {
    value.max(1)
}
