// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resumable bounded proof before deleting one independently addressed page.

use crowdb_protocol::chunk_stream::{
    decode_stream_directory_record, StreamDirectoryControl, StreamDirectoryPageId, StreamDirectoryRef,
    StreamPublicationId, StreamRootIdentity, StreamRootPinId,
};
use crowdb_protocol::key::{BinaryKey, StreamDirectoryPageKey};

use super::{kv_error, DirectoryHeadSnapshot, DirectoryMetadataStore, GetOutcome};
use crate::{Result, StreamError, MAX_DIRECTORY_SCAN_KEYS};

mod deletion;
mod protection;
mod reachability;

/// Opaque proof cursor. A retention change invalidates every partial observation;
/// restarting loses no protection and requires no persisted historical mark set.
#[derive(Debug)]
pub struct DirectoryPageCollection {
    scope: StreamDirectoryControl,
    store_id: u64,
    group_id: u64,
    control_revision: u64,
    target: StreamDirectoryRef,
    current_root: Option<StreamDirectoryRef>,
    stage: CollectionStage,
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
pub enum DirectoryPageCollectionStep {
    Pending(Box<DirectoryPageCollection>),
    Retained,
    Deleted,
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
    ) -> Result<Option<Box<DirectoryPageCollection>>> {
        if expected.store_id != self.store_id || expected.head.metadata_group_id != self.group_id {
            return Err(StreamError::InvalidRequest(
                "page collection binding is invalid".into(),
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
        Ok(Some(Box::new(DirectoryPageCollection {
            scope,
            store_id: self.store_id,
            group_id: self.group_id,
            control_revision,
            target,
            current_root: expected.head.root,
            stage: CollectionStage::Current,
        })))
    }

    /// Advances at most `max_protections` root/pin/publication observations. Each
    /// root lookup follows at most one height-bounded path, never the whole tree.
    /// # Errors
    /// Rejects invalid budgets/bindings, stale retention, corrupt or missing proof
    /// records, and unresolved writes. On error discard the cursor and restart.
    pub async fn advance_directory_page_collection(
        &self,
        mut cursor: Box<DirectoryPageCollection>,
        max_protections: usize,
    ) -> Result<DirectoryPageCollectionStep> {
        if cursor.store_id != self.store_id
            || cursor.group_id != self.group_id
            || max_protections == 0
            || max_protections > MAX_DIRECTORY_SCAN_KEYS
        {
            return Err(StreamError::InvalidRequest(
                "page collection binding or budget is invalid".into(),
            ));
        }
        let (control, revision) = self.load_directory_control(cursor.scope).await?;
        if revision == 0 || revision != cursor.control_revision {
            return Err(StreamError::Backpressure);
        }
        if self.check_page_protections(&mut cursor, max_protections).await? {
            return Ok(DirectoryPageCollectionStep::Retained);
        }
        if !matches!(cursor.stage, CollectionStage::Delete) {
            return Ok(DirectoryPageCollectionStep::Pending(cursor));
        }
        self.delete_unreachable_directory_page(control, &cursor).await?;
        Ok(DirectoryPageCollectionStep::Deleted)
    }
}
