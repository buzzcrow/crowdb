// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Blocking native storage construction for partition recovery.

use super::{tree, CrowdbPartitionTree, PartitionJournal, PartitionTree, StreamPartitionJournal};
use crate::{Checkpoint, ChunkKvError, PartitionRange, Result};
use crowdb_chunk_stream::ChunkStream;
use std::sync::Arc;

pub(super) fn native_storage_parts(
    tree_id: u64,
    range: &PartitionRange,
    mut tree_config: crowdb_tree_ffi::Config,
    page_store: Arc<crowdb_tree_ffi::PageStore>,
    stream: ChunkStream,
) -> Result<(Arc<dyn PartitionTree>, Arc<dyn PartitionJournal>)> {
    tree_config.page_store = Some(page_store);
    tree_config.key_range = crowdb_tree_ffi::KeyRange::Bounded {
        start: range.start.clone(),
        end: range.end.clone(),
    };
    let tree: Arc<dyn PartitionTree> = Arc::new(CrowdbPartitionTree::open(tree_id, &tree_config)?);
    let stream_name = stream.stream_name();
    let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, stream_name)?);
    Ok((tree, journal))
}

pub(super) async fn open_storage(
    tree_id: u64,
    range: PartitionRange,
    tree_config: crowdb_tree_ffi::Config,
    page_store: Arc<crowdb_tree_ffi::PageStore>,
    stream: ChunkStream,
) -> Result<(Arc<dyn PartitionTree>, Arc<dyn PartitionJournal>)> {
    tokio::task::spawn_blocking(move || {
        native_storage_parts(tree_id, &range, tree_config, page_store, stream)
    })
    .await
    .map_err(|error| ChunkKvError::TreeUnavailable(error.to_string()))?
}

pub(super) async fn open_latest(
    tree_id: u64,
    range: PartitionRange,
    tree_config: crowdb_tree_ffi::Config,
    page_store: Arc<crowdb_tree_ffi::PageStore>,
    stream: ChunkStream,
) -> Result<(Arc<dyn PartitionTree>, Arc<dyn PartitionJournal>, Checkpoint)> {
    tokio::task::spawn_blocking(move || {
        let replay_offset = if page_store.is_chunk_backed() && tree::chunk_root_generation(&page_store)? == 0
        {
            0
        } else {
            page_store.wal_replay_offset().map_err(|error| match error {
                crowdb_tree_ffi::CtError::Corruption => ChunkKvError::TreeCorruption(error.to_string()),
                _ => ChunkKvError::TreeUnavailable(error.to_string()),
            })?
        };
        let stream_name = stream.stream_name();
        let stream_generation = stream.record_generation();
        let (tree, journal) = native_storage_parts(tree_id, &range, tree_config, page_store, stream)?;
        let (tree_manifest, applied_seq) = tree.checkpoint_state()?;
        let root_manifest_generation = tree.root_manifest_generation()?;
        Ok((
            tree,
            journal,
            Checkpoint {
                tree_id,
                tree_manifest,
                root_manifest_generation,
                applied_seq,
                stream_name,
                stream_generation,
                replay_offset,
            },
        ))
    })
    .await
    .map_err(|error| ChunkKvError::TreeUnavailable(error.to_string()))?
}
