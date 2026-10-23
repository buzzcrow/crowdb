// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::StreamDirectoryHead;

use super::CandidateRecords;
use crate::directory::{invalid_directory, DirectoryMutation};
use crate::{Result, StreamDirectoryStore};

impl DirectoryMutation {
    pub(super) async fn validate_sealed_span(
        &self,
        store: &dyn StreamDirectoryStore,
        head: &StreamDirectoryHead,
        base: Option<&StreamDirectoryHead>,
    ) -> Result<()> {
        let ended = base
            .and_then(|base| base.active)
            .filter(|previous| head.active.map(|active| active.chunk_id) != Some(previous.chunk_id));
        let (span, previous) = match (self.sealed_span, ended) {
            (None, None) => return Ok(()),
            (Some(span), Some(previous)) => (span, previous),
            _ => {
                return Err(invalid_directory(
                    "attachment transition requires exactly one sealed span",
                ))
            }
        };
        let successor_start = head
            .active
            .map_or(head.published_tail, |active| active.logical_start);
        if span.chunk_id != previous.chunk_id
            || span.logical_start != previous.logical_start
            || span.logical_end != successor_start
            || span.logical_end < previous.checkpoint_logical
            || span.physical_end < previous.checkpoint_physical
            || (span.logical_end == previous.checkpoint_logical
                && span.physical_end != previous.checkpoint_physical)
        {
            return Err(invalid_directory(
                "sealed span differs from original attachment or successor",
            ));
        }
        if span.logical_end <= head.trim_offset {
            return Ok(());
        }
        let records = CandidateRecords {
            candidate: self,
            store,
        };
        let last = self
            .index
            .lookup(&records, span.logical_end - 1)
            .await?
            .ok_or_else(|| invalid_directory("sealed chunk end is absent from the candidate directory"))?;
        if last.location.chunk_id != span.chunk_id
            || last.end_logical().map_err(invalid_directory)? != span.logical_end
            || last.end_physical().map_err(invalid_directory)? != span.physical_end
        {
            return Err(invalid_directory(
                "sealed span differs from its complete final append",
            ));
        }
        Ok(())
    }
}
