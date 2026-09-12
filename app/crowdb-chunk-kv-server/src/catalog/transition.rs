// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, SplitPhase, SplitTransition, TransferPhase, TransferTransition,
};

use super::{ChunkKvRangeCatalogError, ChunkKvRangeCatalogPublisher, ChunkKvRangeCatalogStore};

/// Publishes transfer and split cutovers from durable, readiness-proven plans.
pub struct ChunkKvRangeCatalogCutover {
    publisher: ChunkKvRangeCatalogPublisher,
}

impl ChunkKvRangeCatalogCutover {
    #[must_use]
    pub fn new(store: Arc<dyn ChunkKvRangeCatalogStore>) -> Self {
        Self {
            publisher: ChunkKvRangeCatalogPublisher::new(store),
        }
    }

    /// Atomically changes one partition owner while preserving its artifacts.
    ///
    /// # Errors
    ///
    /// Returns an error when the transition is not prepared, does not match the
    /// current catalog, or the immutable successor cannot be published.
    pub async fn publish_transfer(
        &self,
        transition: &TransferTransition,
    ) -> Result<u64, ChunkKvRangeCatalogError> {
        transition.validate()?;
        if !matches!(
            transition.phase,
            TransferPhase::TargetPrepared | TransferPhase::CatalogCommitted
        ) {
            return Err(ChunkKvRangeCatalogError::TransitionNotReady);
        }
        let (head, pages) = self
            .publisher
            .load_current()
            .await?
            .ok_or(ChunkKvRangeCatalogError::TransitionConflict)?;
        let desired = transfer_entry(transition);
        if exact_entries(&pages, |entry| entry == &desired) == 1 {
            reject_reused_transition_id(&pages, transition.transition_id, 1)?;
            return Ok(head.generation);
        }
        reject_reused_transition_id(&pages, transition.transition_id, 0)?;
        let source = |entry: &ChunkKvRangeCatalogEntry| {
            entry.partition_id == transition.partition_id
                && entry.range == transition.range
                && entry.owner == transition.source
                && entry.owner_epoch == transition.source_epoch
                && entry.state == ChunkKvRangeCatalogPartitionState::Serving
                && entry.artifact == transition.artifact
                && entry.transition_id.is_none()
        };
        let (next_head, next_pages) = replace_one(head, pages, source, vec![desired])?;
        let generation = next_head.generation;
        self.publisher.publish(next_head, next_pages).await?;
        Ok(generation)
    }

    /// Atomically replaces one parent range with two prepared child ranges.
    ///
    /// # Errors
    ///
    /// Returns an error when the transition is not prepared, does not match the
    /// current catalog, or the immutable successor cannot be published.
    pub async fn publish_split(&self, transition: &SplitTransition) -> Result<u64, ChunkKvRangeCatalogError> {
        transition.validate()?;
        if !matches!(
            transition.phase,
            SplitPhase::ChildrenPrepared | SplitPhase::CatalogCommitted
        ) {
            return Err(ChunkKvRangeCatalogError::TransitionNotReady);
        }
        let (head, pages) = self
            .publisher
            .load_current()
            .await?
            .ok_or(ChunkKvRangeCatalogError::TransitionConflict)?;
        let desired = split_entries(transition);
        if desired
            .iter()
            .all(|wanted| exact_entries(&pages, |entry| entry == wanted) == 1)
        {
            reject_reused_transition_id(&pages, transition.transition_id, 2)?;
            return Ok(head.generation);
        }
        reject_reused_transition_id(&pages, transition.transition_id, 0)?;
        let parent = |entry: &ChunkKvRangeCatalogEntry| {
            entry.partition_id == transition.parent_id
                && entry.range == transition.parent_range
                && entry.owner == transition.parent_owner
                && entry.owner_epoch == transition.parent_epoch
                && entry.state == ChunkKvRangeCatalogPartitionState::Serving
                && entry.artifact == transition.parent_artifact
                && entry.transition_id.is_none()
        };
        let (next_head, next_pages) = replace_one(head, pages, parent, desired)?;
        let generation = next_head.generation;
        self.publisher.publish(next_head, next_pages).await?;
        Ok(generation)
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
) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>), ChunkKvRangeCatalogError>
where
    F: Fn(&ChunkKvRangeCatalogEntry) -> bool,
{
    let mut found = None;
    for (page_offset, page) in pages.iter().enumerate() {
        for (entry_offset, entry) in page.entries.iter().enumerate() {
            if matches(entry) && found.replace((page_offset, entry_offset)).is_some() {
                return Err(ChunkKvRangeCatalogError::TransitionConflict);
            }
        }
    }
    let (page_offset, entry_offset) = found.ok_or(ChunkKvRangeCatalogError::TransitionConflict)?;
    let generation = head
        .generation
        .checked_add(1)
        .ok_or(ChunkKvRangeCatalogError::GenerationOverflow)?;
    let changed = &mut pages[page_offset];
    changed.generation = generation;
    changed.entries.splice(entry_offset..=entry_offset, replacement);
    changed.seal()?;

    let mut references = head.pages;
    references[page_offset] = page_reference(changed);
    let mut next = ChunkKvRangeCatalogHead {
        generation,
        previous_generation: Some(head.generation),
        pages: references,
        checksum: [0; 32],
    };
    next.seal()?;
    next.validate_pages(&pages)?;
    Ok((next, pages))
}

fn page_reference(page: &ChunkKvRangeCatalogPage) -> ChunkKvRangeCatalogPageRef {
    ChunkKvRangeCatalogPageRef {
        page_generation: page.generation,
        page_index: page.page_index,
        first_key: page.entries[0].range.start.clone(),
        page_checksum: page.checksum,
    }
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
) -> Result<(), ChunkKvRangeCatalogError> {
    let actual = exact_entries(pages, |entry| entry.transition_id == Some(transition_id));
    if actual == expected {
        Ok(())
    } else {
        Err(ChunkKvRangeCatalogError::TransitionConflict)
    }
}
