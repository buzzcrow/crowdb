// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable chunk-KV range-catalog loading and transition cutover.

use bytes::Bytes;
use crowdb_kv::cluster::group_operations::KvGroupOperationError;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, SplitPhase, SplitTransition, TransferPhase, TransferTransition,
};
use crowdb_protocol::key::{ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, TextKey};

use crate::group0_control_plane::Group0ControlPlane;

pub struct VersionedRangeCatalog {
    pub head: ChunkKvRangeCatalogHead,
    pub pages: Vec<ChunkKvRangeCatalogPage>,
    head_revision: u64,
}

pub async fn load_current(control: &Group0ControlPlane) -> Result<Option<VersionedRangeCatalog>, String> {
    let head_path = ChunkKvRangeCatalogHeadKey.to_path();
    let head_read = control
        .get(head_path.as_bytes())
        .await
        .map_err(|error| operation_error(&error))?;
    let Some(head_bytes) = head_read.value else {
        return Ok(None);
    };
    let head: ChunkKvRangeCatalogHead =
        serde_json::from_slice(&head_bytes).map_err(|error| error.to_string())?;
    let mut pages = Vec::with_capacity(head.pages.len());
    for reference in &head.pages {
        let path = ChunkKvRangeCatalogPageKey {
            generation: reference.page_generation,
            page_index: reference.page_index,
        }
        .to_path();
        let read = control
            .get(path.as_bytes())
            .await
            .map_err(|error| operation_error(&error))?;
        let bytes = read
            .value
            .ok_or_else(|| format!("range catalog page is missing: {path}"))?;
        pages.push(serde_json::from_slice(&bytes).map_err(|error| error.to_string())?);
    }
    head.validate_pages(&pages).map_err(|error| error.to_string())?;
    Ok(Some(VersionedRangeCatalog {
        head,
        pages,
        head_revision: head_read.revision,
    }))
}

pub async fn publish_transfer(
    control: &Group0ControlPlane,
    transition: &TransferTransition,
) -> Result<u64, String> {
    transition.validate().map_err(|error| error.to_string())?;
    if !matches!(
        transition.phase,
        TransferPhase::TargetCatchingUp
            | TransferPhase::CatchupPublished
            | TransferPhase::TargetReady
            | TransferPhase::CatalogCommitted
    ) {
        return Err("transfer is not ready for range catalog publication".into());
    }
    let catalog = load_current(control)
        .await?
        .ok_or_else(|| "range catalog is not published".to_string())?;
    let desired = transfer_entry(transition);
    if exact_entries(&catalog.pages, |entry| entry == &desired) == 1 {
        reject_reused_transition_id(&catalog.pages, transition.transition_id, 1)?;
        return Ok(catalog.head.generation);
    }
    let advancing = matches!(
        transition.phase,
        TransferPhase::TargetReady | TransferPhase::CatalogCommitted
    );
    reject_reused_transition_id(&catalog.pages, transition.transition_id, usize::from(advancing))?;
    let source = |entry: &ChunkKvRangeCatalogEntry| {
        let source_serving = entry.owner == transition.source
            && entry.owner_epoch == transition.source_epoch
            && entry.state == ChunkKvRangeCatalogPartitionState::Serving
            && entry.artifact == transition.artifact
            && entry.transition_id.is_none();
        let target_catching_up = entry.owner == transition.target
            && entry.owner_epoch == transition.target_epoch
            && entry.state == ChunkKvRangeCatalogPartitionState::TargetCatchingUp
            && entry.artifact == transition.target_artifact
            && entry.transition_id == Some(transition.transition_id);
        entry.partition_id == transition.partition_id
            && entry.range == transition.range
            && if advancing {
                target_catching_up
            } else {
                source_serving
            }
    };
    let (head, pages) = replace_one(catalog.head, catalog.pages, source, vec![desired])?;
    publish(control, head, pages, catalog.head_revision).await
}

pub async fn publish_materialized_partition(
    control: &Group0ControlPlane,
    partition_id: crowdb_protocol::chunk_kv::Id128,
) -> Result<u64, String> {
    let catalog = load_current(control)
        .await?
        .ok_or_else(|| "range catalog is not published".to_string())?;
    let Some(current) = catalog
        .pages
        .iter()
        .flat_map(|page| &page.entries)
        .find(|entry| entry.partition_id == partition_id)
        .cloned()
    else {
        return Err("materialized partition is absent from the catalog".into());
    };
    if current.artifact.tail_overlay.is_none() {
        return Ok(catalog.head.generation);
    }
    let transition_id = current
        .transition_id
        .ok_or_else(|| "materialized split child has no transition identity".to_string())?;
    let (head, pages) = release_materialized_split(catalog.head, catalog.pages, transition_id, partition_id)?;
    publish(control, head, pages, catalog.head_revision).await
}

pub async fn publish_materialized_transfer(
    control: &Group0ControlPlane,
    partition_id: crowdb_protocol::chunk_kv::Id128,
) -> Result<u64, String> {
    let catalog = load_current(control)
        .await?
        .ok_or_else(|| "range catalog is not published".to_string())?;
    let Some(current) = catalog
        .pages
        .iter()
        .flat_map(|page| &page.entries)
        .find(|entry| entry.partition_id == partition_id)
    else {
        return Err("materialized transfer target is absent from the catalog".into());
    };
    if current.artifact.tail_overlay.is_none() && current.transition_id.is_none() {
        return Ok(catalog.head.generation);
    }
    let transition_id = current
        .transition_id
        .ok_or_else(|| "materialized transfer target has no transition identity".to_string())?;
    let (head, pages) =
        release_materialized_transfer(catalog.head, catalog.pages, transition_id, partition_id)?;
    publish(control, head, pages, catalog.head_revision).await
}

fn release_materialized_transfer(
    head: ChunkKvRangeCatalogHead,
    mut pages: Vec<ChunkKvRangeCatalogPage>,
    transition_id: crowdb_protocol::chunk_kv::Id128,
    partition_id: crowdb_protocol::chunk_kv::Id128,
) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>), String> {
    let generation = head
        .generation
        .checked_add(1)
        .ok_or_else(|| "range catalog generation overflowed".to_string())?;
    let mut matched = 0;
    for page in &mut pages {
        let mut changed = false;
        for entry in &mut page.entries {
            if entry.transition_id != Some(transition_id) {
                continue;
            }
            matched += 1;
            if entry.partition_id != partition_id || entry.artifact.tail_overlay.is_none() {
                return Err("materialized transfer target identity is inconsistent".into());
            }
            entry.artifact.tail_overlay = None;
            entry.transition_id = None;
            changed = true;
        }
        if changed {
            page.generation = generation;
            page.seal().map_err(|error| error.to_string())?;
        }
    }
    if matched != 1 {
        return Err("materialized transfer must match exactly one catalog entry".into());
    }
    let mut references = head.pages;
    for page in pages.iter().filter(|page| page.generation == generation) {
        let reference = references
            .iter_mut()
            .find(|reference| reference.page_index == page.page_index)
            .ok_or_else(|| "changed range catalog page has no head reference".to_string())?;
        *reference = page_reference(page)?;
    }
    let mut next = ChunkKvRangeCatalogHead {
        generation,
        previous_generation: Some(head.generation),
        pages: references,
        checksum: [0; 32],
    };
    next.seal().map_err(|error| error.to_string())?;
    next.validate_pages(&pages).map_err(|error| error.to_string())?;
    Ok((next, pages))
}

fn release_materialized_split(
    head: ChunkKvRangeCatalogHead,
    mut pages: Vec<ChunkKvRangeCatalogPage>,
    transition_id: crowdb_protocol::chunk_kv::Id128,
    partition_id: crowdb_protocol::chunk_kv::Id128,
) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>), String> {
    let generation = head
        .generation
        .checked_add(1)
        .ok_or_else(|| "range catalog generation overflowed".to_string())?;
    let mut matched = 0;
    let mut partition_matched = false;
    for page in &mut pages {
        let mut changed = false;
        for entry in &mut page.entries {
            if entry.transition_id != Some(transition_id) {
                continue;
            }
            matched += 1;
            if entry.partition_id == partition_id {
                if entry.artifact.tail_overlay.is_none() {
                    return Err("materialized split partition overlay is absent".into());
                }
                entry.artifact.tail_overlay = None;
                partition_matched = true;
                changed = true;
            }
        }
        if changed {
            page.generation = generation;
        }
    }
    if matched != 2 || !partition_matched {
        return Err("materialized split must match one partition in an exact writer pair".into());
    }
    let all_materialized = pages
        .iter()
        .flat_map(|page| &page.entries)
        .filter(|entry| entry.transition_id == Some(transition_id))
        .all(|entry| entry.artifact.tail_overlay.is_none());
    if all_materialized {
        for page in &mut pages {
            let mut changed = false;
            for entry in &mut page.entries {
                if entry.transition_id == Some(transition_id) {
                    entry.transition_id = None;
                    changed = true;
                }
            }
            if changed {
                page.generation = generation;
            }
        }
    }
    for page in pages.iter_mut().filter(|page| page.generation == generation) {
        page.seal().map_err(|error| error.to_string())?;
    }
    let mut references = head.pages;
    for page in pages.iter().filter(|page| page.generation == generation) {
        let reference = references
            .iter_mut()
            .find(|reference| reference.page_index == page.page_index)
            .ok_or_else(|| "changed range catalog page has no head reference".to_string())?;
        *reference = page_reference(page)?;
    }
    let mut next = ChunkKvRangeCatalogHead {
        generation,
        previous_generation: Some(head.generation),
        pages: references,
        checksum: [0; 32],
    };
    next.seal().map_err(|error| error.to_string())?;
    next.validate_pages(&pages).map_err(|error| error.to_string())?;
    Ok((next, pages))
}

pub async fn publish_split(
    control: &Group0ControlPlane,
    transition: &SplitTransition,
) -> Result<u64, String> {
    transition.validate().map_err(|error| error.to_string())?;
    if !matches!(
        transition.phase,
        SplitPhase::ChildPrepared | SplitPhase::CatalogCommitted
    ) {
        return Err("split is not ready for range catalog publication".into());
    }
    let catalog = load_current(control)
        .await?
        .ok_or_else(|| "range catalog is not published".to_string())?;
    let desired = split_entries(transition);
    if desired
        .iter()
        .all(|wanted| exact_entries(&catalog.pages, |entry| entry == wanted) == 1)
    {
        reject_reused_transition_id(&catalog.pages, transition.transition_id, 2)?;
        return Ok(catalog.head.generation);
    }
    reject_reused_transition_id(&catalog.pages, transition.transition_id, 0)?;
    let parent = |entry: &ChunkKvRangeCatalogEntry| {
        entry.partition_id == transition.parent_id
            && entry.range == transition.parent_range
            && entry.owner == transition.parent_owner
            && entry.owner_epoch == transition.parent_epoch
            && entry.state == ChunkKvRangeCatalogPartitionState::Serving
            && entry.artifact == transition.parent_artifact
    };
    let (head, pages) = replace_one(catalog.head, catalog.pages, parent, desired)?;
    publish(control, head, pages, catalog.head_revision).await
}

async fn publish(
    control: &Group0ControlPlane,
    head: ChunkKvRangeCatalogHead,
    pages: Vec<ChunkKvRangeCatalogPage>,
    expected_head_revision: u64,
) -> Result<u64, String> {
    let generation = head.generation;
    for page in pages.iter().filter(|page| page.generation == generation) {
        let path = ChunkKvRangeCatalogPageKey {
            generation: page.generation,
            page_index: page.page_index,
        }
        .to_path();
        let encoded = serde_json::to_vec(page).map_err(|error| error.to_string())?;
        match control
            .compare_and_put(Bytes::from(path.clone()), Bytes::from(encoded), 0)
            .await
        {
            Ok(_) => {}
            Err(KvGroupOperationError::CompareFailed { .. }) => {
                let read = control
                    .get(path.as_bytes())
                    .await
                    .map_err(|error| operation_error(&error))?;
                let stored: ChunkKvRangeCatalogPage = serde_json::from_slice(
                    read.value
                        .as_deref()
                        .ok_or_else(|| "immutable range catalog page disappeared".to_string())?,
                )
                .map_err(|error| error.to_string())?;
                if stored != *page {
                    return Err("immutable range catalog page conflicts".into());
                }
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    let head_path = ChunkKvRangeCatalogHeadKey.to_path();
    let encoded = serde_json::to_vec(&head).map_err(|error| error.to_string())?;
    match control
        .compare_and_put(
            Bytes::from(head_path.clone()),
            Bytes::from(encoded),
            expected_head_revision,
        )
        .await
    {
        Ok(_) => Ok(generation),
        Err(KvGroupOperationError::CompareFailed { .. }) => {
            let current = load_current(control).await?;
            if current.as_ref().is_some_and(|catalog| catalog.head == head) {
                Ok(generation)
            } else {
                Err("range catalog head changed concurrently".into())
            }
        }
        Err(error) => Err(error.to_string()),
    }
}

fn transfer_entry(transition: &TransferTransition) -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id: transition.partition_id,
        range: transition.range.clone(),
        owner: transition.target.clone(),
        owner_epoch: transition.target_epoch,
        state: if matches!(
            transition.phase,
            TransferPhase::TargetCatchingUp | TransferPhase::CatchupPublished
        ) {
            ChunkKvRangeCatalogPartitionState::TargetCatchingUp
        } else {
            ChunkKvRangeCatalogPartitionState::Serving
        },
        artifact: transition.target_artifact.clone(),
        transition_id: Some(transition.transition_id),
    }
}

fn split_entries(transition: &SplitTransition) -> Vec<ChunkKvRangeCatalogEntry> {
    vec![
        ChunkKvRangeCatalogEntry {
            partition_id: transition.parent_id,
            range: crowdb_protocol::chunk_kv::KeyRange {
                start: transition.parent_range.start.clone(),
                end: Some(transition.split_key.clone()),
            },
            owner: transition.parent_owner.clone(),
            owner_epoch: transition.parent_next_epoch,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: transition.retained_parent_artifact.clone(),
            transition_id: Some(transition.transition_id),
        },
        ChunkKvRangeCatalogEntry {
            partition_id: transition.child.partition_id,
            range: transition.child.range.clone(),
            owner: transition.child.owner.clone(),
            owner_epoch: transition.child.owner_epoch,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: transition.child.artifact.clone(),
            transition_id: Some(transition.transition_id),
        },
    ]
}

fn replace_one<F>(
    head: ChunkKvRangeCatalogHead,
    mut pages: Vec<ChunkKvRangeCatalogPage>,
    matches: F,
    replacement: Vec<ChunkKvRangeCatalogEntry>,
) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>), String>
where
    F: Fn(&ChunkKvRangeCatalogEntry) -> bool,
{
    let mut found = None;
    for (page_offset, page) in pages.iter().enumerate() {
        for (entry_offset, entry) in page.entries.iter().enumerate() {
            if matches(entry) && found.replace((page_offset, entry_offset)).is_some() {
                return Err("range catalog transition matched multiple entries".into());
            }
        }
    }
    let (page_offset, entry_offset) =
        found.ok_or_else(|| "range catalog transition source is missing".to_string())?;
    let generation = head
        .generation
        .checked_add(1)
        .ok_or_else(|| "range catalog generation overflowed".to_string())?;
    let changed = &mut pages[page_offset];
    changed.generation = generation;
    changed.entries.splice(entry_offset..=entry_offset, replacement);
    changed.seal().map_err(|error| error.to_string())?;
    let mut references = head.pages;
    references[page_offset] = page_reference(changed)?;
    let mut next = ChunkKvRangeCatalogHead {
        generation,
        previous_generation: Some(head.generation),
        pages: references,
        checksum: [0; 32],
    };
    next.seal().map_err(|error| error.to_string())?;
    next.validate_pages(&pages).map_err(|error| error.to_string())?;
    Ok((next, pages))
}

fn page_reference(page: &ChunkKvRangeCatalogPage) -> Result<ChunkKvRangeCatalogPageRef, String> {
    Ok(ChunkKvRangeCatalogPageRef {
        page_generation: page.generation,
        page_index: page.page_index,
        first_key: page
            .entries
            .first()
            .ok_or_else(|| "range catalog page became empty".to_string())?
            .range
            .start
            .clone(),
        page_checksum: page.checksum,
    })
}

fn exact_entries<F>(pages: &[ChunkKvRangeCatalogPage], matches: F) -> usize
where
    F: Fn(&ChunkKvRangeCatalogEntry) -> bool,
{
    pages
        .iter()
        .flat_map(|page| &page.entries)
        .filter(|entry| matches(entry))
        .count()
}

fn reject_reused_transition_id(
    pages: &[ChunkKvRangeCatalogPage],
    transition_id: crowdb_protocol::chunk_kv::Id128,
    expected: usize,
) -> Result<(), String> {
    let actual = exact_entries(pages, |entry| entry.transition_id == Some(transition_id));
    if actual == expected {
        Ok(())
    } else {
        Err("range catalog transition identity was reused".into())
    }
}

fn operation_error(error: &KvGroupOperationError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use crowdb_protocol::chunk_kv::{
        Id128, KeyRange, OwnerDescriptor, PartitionArtifact, TailOverlayArtifact,
    };
    use crowdb_protocol::chunk_stream::StreamName;

    use super::*;

    #[test]
    fn materialization_releases_each_overlay_before_completed_split_identity() {
        let transition_id = Id128 { high: 7, low: 8 };
        let parent_id = Id128 { high: 1, low: 1 };
        let child_id = Id128 { high: 1, low: 2 };
        let child = ChunkKvRangeCatalogEntry {
            partition_id: child_id,
            range: KeyRange {
                start: b"m".to_vec(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 3,
                rpc_endpoint: "127.0.0.1:9003".into(),
            },
            owner_epoch: 4,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 5,
                stream_name: StreamName { high: 6, low: 7 },
                tail_overlay: Some(TailOverlayArtifact {
                    source_partition_id: Id128 { high: 9, low: 10 },
                    source_epoch: 3,
                    source_stream_name: StreamName { high: 11, low: 12 },
                    source_stream_manifest_generation: 1,
                    replay_offset: 0,
                    cutover_offset: 16,
                    base_root_manifest_generation: 1,
                    base_tree_manifest: 1,
                    base_applied_seq: 2,
                    cutover_seq: 3,
                    target_stream_start_seq: 4,
                }),
            },
            transition_id: Some(transition_id),
        };
        let mut parent = child.clone();
        parent.partition_id = parent_id;
        parent.range = KeyRange {
            start: Vec::new(),
            end: Some(b"m".to_vec()),
        };
        parent.artifact.tree_id = 4;
        let mut page = ChunkKvRangeCatalogPage {
            generation: 1,
            page_index: 0,
            entries: vec![parent, child],
            checksum: [0; 32],
        };
        page.seal().unwrap();
        let mut head = ChunkKvRangeCatalogHead {
            generation: 1,
            previous_generation: None,
            pages: vec![page_reference(&page).unwrap()],
            checksum: [0; 32],
        };
        head.seal().unwrap();

        let (head, pages) = release_materialized_split(head, vec![page], transition_id, parent_id).unwrap();
        let parent = pages[0]
            .entries
            .iter()
            .find(|entry| entry.partition_id == parent_id)
            .unwrap();
        let child = pages[0]
            .entries
            .iter()
            .find(|entry| entry.partition_id == child_id)
            .unwrap();
        assert!(parent.artifact.tail_overlay.is_none());
        assert!(child.artifact.tail_overlay.is_some());
        assert_eq!(parent.transition_id, Some(transition_id));
        assert_eq!(child.transition_id, Some(transition_id));

        let (_, pages) = release_materialized_split(head, pages, transition_id, child_id).unwrap();

        assert!(pages[0]
            .entries
            .iter()
            .all(|entry| entry.artifact.tail_overlay.is_none() && entry.transition_id.is_none()));
    }

    #[test]
    fn materialization_releases_one_completed_transfer_identity() {
        let transition_id = Id128 { high: 17, low: 18 };
        let partition_id = Id128 { high: 2, low: 1 };
        let mut entry = ChunkKvRangeCatalogEntry {
            partition_id,
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 3,
                rpc_endpoint: "127.0.0.1:9003".into(),
            },
            owner_epoch: 4,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 5,
                stream_name: StreamName { high: 6, low: 7 },
                tail_overlay: Some(TailOverlayArtifact {
                    source_partition_id: partition_id,
                    source_epoch: 3,
                    source_stream_name: StreamName { high: 8, low: 9 },
                    source_stream_manifest_generation: 1,
                    replay_offset: 0,
                    cutover_offset: 16,
                    base_root_manifest_generation: 1,
                    base_tree_manifest: 1,
                    base_applied_seq: 2,
                    cutover_seq: 3,
                    target_stream_start_seq: 4,
                }),
            },
            transition_id: Some(transition_id),
        };
        let mut page = ChunkKvRangeCatalogPage {
            generation: 1,
            page_index: 0,
            entries: vec![entry.clone()],
            checksum: [0; 32],
        };
        page.seal().unwrap();
        let mut head = ChunkKvRangeCatalogHead {
            generation: 1,
            previous_generation: None,
            pages: vec![page_reference(&page).unwrap()],
            checksum: [0; 32],
        };
        head.seal().unwrap();

        let (head, pages) =
            release_materialized_transfer(head, vec![page], transition_id, partition_id).unwrap();
        entry.artifact.tail_overlay = None;
        entry.transition_id = None;
        assert_eq!(head.generation, 2);
        assert_eq!(pages[0].entries, vec![entry]);
    }
}
