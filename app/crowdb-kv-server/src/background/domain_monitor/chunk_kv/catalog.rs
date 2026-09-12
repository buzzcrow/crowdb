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
        TransferPhase::TargetPrepared | TransferPhase::CatalogCommitted
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
    reject_reused_transition_id(&catalog.pages, transition.transition_id, 0)?;
    let source = |entry: &ChunkKvRangeCatalogEntry| {
        entry.partition_id == transition.partition_id
            && entry.range == transition.range
            && entry.owner == transition.source
            && entry.owner_epoch == transition.source_epoch
            && entry.state == ChunkKvRangeCatalogPartitionState::Serving
            && entry.artifact == transition.artifact
    };
    let (head, pages) = replace_one(catalog.head, catalog.pages, source, vec![desired])?;
    publish(control, head, pages, catalog.head_revision).await
}

pub async fn publish_split(
    control: &Group0ControlPlane,
    transition: &SplitTransition,
) -> Result<u64, String> {
    transition.validate().map_err(|error| error.to_string())?;
    if !matches!(
        transition.phase,
        SplitPhase::ChildrenPrepared | SplitPhase::CatalogCommitted
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
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact: transition.artifact.clone(),
        transition_id: Some(transition.transition_id),
    }
}

fn split_entries(transition: &SplitTransition) -> Vec<ChunkKvRangeCatalogEntry> {
    [&transition.left, &transition.right]
        .into_iter()
        .map(|child| ChunkKvRangeCatalogEntry {
            partition_id: child.partition_id,
            range: child.range.clone(),
            owner: child.owner.clone(),
            owner_epoch: child.owner_epoch,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: child.artifact.clone(),
            transition_id: Some(transition.transition_id),
        })
        .collect()
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
