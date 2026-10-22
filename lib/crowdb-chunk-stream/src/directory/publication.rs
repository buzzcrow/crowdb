// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{StreamDirectoryHead, StreamDirectoryPageId, StreamName};

use super::{invalid_directory, DirectoryMutation, Result, StreamDirectoryStore};

struct CandidateRecords<'a> {
    candidate: &'a DirectoryMutation,
    store: &'a dyn StreamDirectoryStore,
}

#[async_trait]
impl StreamDirectoryStore for CandidateRecords<'_> {
    async fn load_directory_record(
        &self,
        name: StreamName,
        id: StreamDirectoryPageId,
    ) -> Result<Option<Bytes>> {
        if let Some(page) = self.candidate.pages.iter().find(|page| page.reference.id == id) {
            return Ok(Some(page.bytes.clone()));
        }
        self.store.load_directory_record(name, id).await
    }
}

impl DirectoryMutation {
    pub(crate) async fn validate_head(
        &self,
        store: &dyn StreamDirectoryStore,
        head: &StreamDirectoryHead,
        base: Option<&StreamDirectoryHead>,
    ) -> Result<()> {
        head.validate().map_err(invalid_directory)?;
        if head.stream_name != self.index.stream_name
            || head.purpose != self.index.purpose
            || head.root != self.index.root
            || base.and_then(|base| base.root) != self.base
        {
            return Err(invalid_directory(
                "candidate head does not match its prepared mutation",
            ));
        }
        let Some(active) = head.active else { return Ok(()) };
        if active.logical_start == active.checkpoint_logical {
            return Ok(());
        }
        let records = CandidateRecords {
            candidate: self,
            store,
        };
        let last = self
            .index
            .lookup(&records, head.published_tail - 1)
            .await?
            .ok_or_else(|| invalid_directory("active checkpoint is absent from the directory"))?;
        if last.location.chunk_id != active.chunk_id
            || last.end_physical().map_err(invalid_directory)? != active.checkpoint_physical
        {
            return Err(invalid_directory(
                "active checkpoint does not match the last location",
            ));
        }
        if head
            .root
            .is_some_and(|root| active.logical_start >= root.first_logical)
        {
            let first = self
                .index
                .lookup(&records, active.logical_start)
                .await?
                .ok_or_else(|| invalid_directory("active chunk start is absent"))?;
            if first.location.chunk_id != active.chunk_id
                || first.logical_start != active.logical_start
                || first.location.frame_offset != 0
            {
                return Err(invalid_directory("active chunk original start is inconsistent"));
            }
        } else if !base.and_then(|base| base.active).is_some_and(|previous| {
            previous.chunk_id == active.chunk_id && previous.logical_start == active.logical_start
        }) {
            return Err(invalid_directory(
                "trimmed active chunk has no matching original attachment",
            ));
        }
        Ok(())
    }
}
