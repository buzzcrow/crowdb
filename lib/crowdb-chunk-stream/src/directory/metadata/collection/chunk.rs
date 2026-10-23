// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A proven span remains durable retry work until exact physical release completes.

use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::{StreamDirectoryControl, StreamSealedChunkSpan};
use crowdb_protocol::key::{BinaryKey, StreamSealedChunkSpanKey};

use super::DirectoryCollection;
use crate::directory::metadata::{kv_error, GetOutcome};
use crate::{DirectoryMetadataStore, Result, StreamChunkStore, StreamError, TrimmedChunk};

#[derive(Debug)]
pub struct DirectoryChunkRelease {
    span: StreamSealedChunkSpan,
    revision: u64,
    store_id: u64,
    group_id: u64,
}

impl DirectoryChunkRelease {
    #[must_use]
    pub const fn span(&self) -> &StreamSealedChunkSpan {
        &self.span
    }
}

impl DirectoryMetadataStore {
    pub(super) async fn prepare_directory_chunk_release(
        &self,
        control: BatchOp,
        cursor: &DirectoryCollection,
        span: StreamSealedChunkSpan,
    ) -> Result<DirectoryChunkRelease> {
        let ops = [control, Self::sealed_span_op(&span)?];
        let BatchOp::Put { key, .. } = &ops[0] else {
            unreachable!()
        };
        match self
            .records
            .batch_write_cas(&ops, key, cursor.control_revision)
            .await
        {
            Ok(outcome) => Ok(DirectoryChunkRelease {
                span,
                revision: outcome.revision,
                store_id: self.store_id,
                group_id: self.group_id,
            }),
            Err(crowdb_kv_client::Error::CasBusy | crowdb_kv_client::Error::CasFailed { .. }) => {
                Err(StreamError::Backpressure)
            }
            Err(crowdb_kv_client::Error::OutcomeUnknown) => Err(StreamError::WriteStalled),
            Err(error) => Err(kv_error(error)),
        }
    }

    /// Releases exactly one proven whole chunk, then removes its durable span.
    /// Both the physical operation and a missing completed record are idempotent.
    /// After a crash, reprove an enumerated span and retry the same physical ID.
    /// # Errors
    /// Rejects wrong/superseded tickets, active or differently sealed physical
    /// chunks, unavailable IO and conflicting metadata. The span remains on failure.
    pub async fn finish_directory_chunk_release(
        &self,
        ticket: DirectoryChunkRelease,
        chunks: &dyn StreamChunkStore,
    ) -> Result<TrimmedChunk> {
        if ticket.store_id != self.store_id || ticket.group_id != self.group_id {
            return Err(StreamError::InvalidRequest(
                "chunk release ticket belongs to another metadata binding".into(),
            ));
        }
        if !self.directory_release_ticket_current(&ticket).await? {
            return Ok(TrimmedChunk {
                chunk_id: ticket.span.chunk_id,
                reclaimed_bytes: 0,
            });
        }
        let released = chunks
            .release_trimmed(
                ticket.span.chunk_id,
                ticket.span.logical_end,
                ticket.span.physical_end,
            )
            .await?;
        if released.chunk_id != ticket.span.chunk_id {
            return Err(StreamError::Corruption(
                "chunk release returned a different identity".into(),
            ));
        }
        let (control, revision) = self
            .load_directory_control(StreamDirectoryControl {
                stream_name: ticket.span.stream_name,
                purpose: ticket.span.purpose,
            })
            .await?;
        if revision == 0 {
            return Err(StreamError::ReadUnavailable(
                "root retention control is missing".into(),
            ));
        }
        if self.directory_release_ticket_current(&ticket).await? {
            let key = StreamSealedChunkSpanKey {
                stream_name: ticket.span.stream_name,
                chunk_id: ticket.span.chunk_id,
            }
            .to_bytes();
            self.delete_retention_records(control, revision, &[key]).await?;
        }
        Ok(released)
    }

    async fn directory_release_ticket_current(&self, ticket: &DirectoryChunkRelease) -> Result<bool> {
        let BatchOp::Put { key, value } = Self::sealed_span_op(&ticket.span)? else {
            unreachable!()
        };
        match self.records.get(&key).await.map_err(kv_error)? {
            GetOutcome::NotFound => Ok(false),
            GetOutcome::Found {
                value: observed,
                revision,
            } => {
                if value != observed {
                    return Err(StreamError::Corruption(
                        "chunk release span changed its immutable bounds".into(),
                    ));
                }
                if revision != ticket.revision {
                    return Err(StreamError::Backpressure);
                }
                Ok(true)
            }
        }
    }
}
