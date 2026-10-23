// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resumable bounded proof before collecting one directory page or sealed chunk.

use crowdb_protocol::chunk_stream::{
    decode_stream_directory_record, StreamDirectoryControl, StreamDirectoryPageId, StreamDirectoryRef,
    StreamPublicationId, StreamRootIdentity, StreamRootPinId, StreamSealedChunkSpan,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::key::{BinaryKey, StreamDirectoryPageKey};

use super::{kv_error, DirectoryHeadSnapshot, DirectoryMetadataStore, GetOutcome};
use crate::{Result, StreamError, MAX_DIRECTORY_SCAN_KEYS};

mod chunk;
mod deletion;
pub use chunk::DirectoryChunkRelease;
mod protection;
mod reachability;

/// Opaque proof cursor. A retention change invalidates every partial observation;
/// restarting loses no protection and requires no persisted historical mark set.
#[derive(Debug)]
pub struct DirectoryCollection {
    scope: StreamDirectoryControl,
    store_id: u64,
    group_id: u64,
    control_revision: u64,
    target: CollectionTarget,
    current_root: Option<StreamDirectoryRef>,
    current_trim: u64,
    current_tail: u64,
    current_active: Option<ChunkId>,
    stage: CollectionStage,
}

#[derive(Clone, Copy, Debug)]
enum CollectionTarget {
    Page(StreamDirectoryRef),
    Chunk(StreamSealedChunkSpan),
}

#[derive(Clone, Copy, Debug)]
enum CollectionStage {
    Current,
    Roots(Option<StreamRootIdentity>),
    Pins(Option<StreamRootPinId>),
    Publications(Option<StreamPublicationId>),
    Delete,
}

#[derive(Debug)]
pub enum DirectoryCollectionStep {
    Pending(Box<DirectoryCollection>),
    Retained,
    PageDeleted,
    ChunkRelease(DirectoryChunkRelease),
}

impl DirectoryMetadataStore {
    /// Starts collection of one enumerated page under the observed current head.
    /// Returns None if the page is already absent. This does not authorize deletion.
    /// # Errors
    /// Rejects stale/wrong-scope heads, corrupt page bytes or missing control.
    pub async fn begin_directory_page_collection(
        &self,
        expected: &DirectoryHeadSnapshot,
        page_id: StreamDirectoryPageId,
    ) -> Result<Option<Box<DirectoryCollection>>> {
        let (scope, control_revision) = self.capture_collection(expected).await?;
        let key = StreamDirectoryPageKey {
            stream_name: scope.stream_name,
            page_id,
        }
        .to_bytes();
        let GetOutcome::Found { value, .. } = self.records.get(&key).await.map_err(kv_error)? else {
            return Ok(None);
        };
        let (target, _) = decode_stream_directory_record(&value, page_id, scope.stream_name, scope.purpose)
            .map_err(|error| StreamError::Corruption(error.to_string()))?;
        Ok(Some(self.collection_cursor(
            expected,
            scope,
            control_revision,
            CollectionTarget::Page(target),
        )))
    }

    /// Starts a whole-chunk retention proof. A successful proof grants one exact
    /// release ticket; the durable span remains until physical release succeeds.
    /// # Errors
    /// Rejects stale/wrong-scope heads, malformed spans or missing control.
    pub async fn begin_directory_chunk_collection(
        &self,
        expected: &DirectoryHeadSnapshot,
        chunk_id: ChunkId,
    ) -> Result<Option<Box<DirectoryCollection>>> {
        let (scope, control_revision) = self.capture_collection(expected).await?;
        let Some(span) = self.load_directory_span(scope.stream_name, chunk_id).await? else {
            return Ok(None);
        };
        if span.purpose != scope.purpose {
            return Err(StreamError::Corruption(
                "chunk collection span purpose is invalid".into(),
            ));
        }
        Ok(Some(self.collection_cursor(
            expected,
            scope,
            control_revision,
            CollectionTarget::Chunk(span),
        )))
    }

    async fn capture_collection(
        &self,
        expected: &DirectoryHeadSnapshot,
    ) -> Result<(StreamDirectoryControl, u64)> {
        if expected.store_id != self.store_id || expected.head.metadata_group_id != self.group_id {
            return Err(StreamError::InvalidRequest(
                "collection binding is invalid".into(),
            ));
        }
        let scope = StreamDirectoryControl {
            stream_name: expected.head.stream_name,
            purpose: expected.head.purpose,
        };
        let (_, control_revision) = self.load_directory_control(scope).await?;
        if control_revision == 0 {
            return Err(StreamError::ReadUnavailable(
                "root retention control is missing".into(),
            ));
        }
        let current = self.load_directory_head(scope.stream_name).await?;
        if !current.is_some_and(|head| head.revision == expected.revision && head.head == expected.head) {
            return Err(StreamError::Backpressure);
        }
        Ok((scope, control_revision))
    }

    fn collection_cursor(
        &self,
        expected: &DirectoryHeadSnapshot,
        scope: StreamDirectoryControl,
        control_revision: u64,
        target: CollectionTarget,
    ) -> Box<DirectoryCollection> {
        Box::new(DirectoryCollection {
            scope,
            store_id: self.store_id,
            group_id: self.group_id,
            control_revision,
            target,
            current_root: expected.head.root,
            current_trim: expected.head.trim_offset,
            current_tail: expected.head.published_tail,
            current_active: expected.head.active.map(|active| active.chunk_id),
            stage: CollectionStage::Current,
        })
    }

    /// Advances at most `max_protections` root/pin/publication observations. Each
    /// root lookup follows at most one height-bounded path, never the whole tree.
    /// # Errors
    /// Rejects invalid budgets/bindings, stale retention, corrupt or missing proof
    /// records, and unresolved writes. On error discard the cursor and restart.
    pub async fn advance_directory_collection(
        &self,
        mut cursor: Box<DirectoryCollection>,
        max_protections: usize,
    ) -> Result<DirectoryCollectionStep> {
        if cursor.store_id != self.store_id
            || cursor.group_id != self.group_id
            || max_protections == 0
            || max_protections > MAX_DIRECTORY_SCAN_KEYS
        {
            return Err(StreamError::InvalidRequest(
                "collection binding or budget is invalid".into(),
            ));
        }
        let (control, revision) = self.load_directory_control(cursor.scope).await?;
        if revision == 0 || revision != cursor.control_revision {
            return Err(StreamError::Backpressure);
        }
        if self
            .check_collection_protections(&mut cursor, max_protections)
            .await?
        {
            return Ok(DirectoryCollectionStep::Retained);
        }
        if !matches!(cursor.stage, CollectionStage::Delete) {
            return Ok(DirectoryCollectionStep::Pending(cursor));
        }
        match cursor.target {
            CollectionTarget::Page(_) => {
                self.delete_unreachable_directory_page(control, &cursor).await?;
                Ok(DirectoryCollectionStep::PageDeleted)
            }
            CollectionTarget::Chunk(span) => {
                let ticket = self
                    .prepare_directory_chunk_release(control, &cursor, span)
                    .await?;
                Ok(DirectoryCollectionStep::ChunkRelease(ticket))
            }
        }
    }
}
