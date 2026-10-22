// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::{
    encode_stream_directory_head, StreamDirectoryControl, StreamDirectoryHead, StreamMigrationPhase,
    StreamMigrationProgress,
};
use crowdb_protocol::key::{
    BinaryKey, StreamDirectoryRootKey, StreamManifestHeadKey, StreamMigrationProgressKey,
};

use super::{kv_error, migration_error, DirectoryMetadataStore, StreamMigrationSnapshot};
use crate::{DirectoryHeadSnapshot, Result, StreamError};

impl DirectoryMetadataStore {
    pub(super) async fn finish_directory_migration(
        &self,
        snapshot: &StreamMigrationSnapshot,
        progress: &StreamMigrationProgress,
    ) -> Result<DirectoryHeadSnapshot> {
        progress
            .validate_source(&snapshot.source_view()?)
            .map_err(migration_error)?;
        if progress.marker != snapshot.marker || progress.phase != StreamMigrationPhase::Complete {
            return Err(StreamError::StaleWriter);
        }
        let source = snapshot.source_view()?;
        let archived = StreamDirectoryHead {
            stream_name: source.stream_name,
            purpose: source.purpose,
            metadata_group_id: source.metadata_group_id,
            writer_epoch: source.writer_epoch,
            owner_session: snapshot.marker.owner_session,
            generation: source.generation,
            trim_offset: source.trim_offset,
            published_tail: source.sealed_tail,
            root: progress.legacy_root,
            active: None,
            previous: None,
            closed: source.closed,
        };
        let head = StreamDirectoryHead {
            writer_epoch: snapshot.marker.writer_epoch,
            generation: snapshot.marker.generation,
            published_tail: progress.logical_tail,
            root: progress.root,
            previous: Some(snapshot.marker.legacy),
            ..archived.clone()
        };
        self.retain_migration_root(&archived).await?;
        let bytes = self.retain_migration_root(&head).await?;
        let key = StreamManifestHeadKey {
            stream_name: head.stream_name,
        }
        .to_bytes();
        let progress_key = StreamMigrationProgressKey {
            stream_name: head.stream_name,
            source: snapshot.marker.source,
        }
        .to_bytes();
        let ops = [
            Self::directory_control_scope_op(StreamDirectoryControl {
                stream_name: head.stream_name,
                purpose: head.purpose,
            })?,
            BatchOp::Put {
                key: Bytes::copy_from_slice(&key),
                value: Bytes::from(bytes),
            },
            BatchOp::Delete {
                key: Bytes::from(progress_key),
            },
        ];
        match self.records.batch_write_cas(&ops, &key, snapshot.revision).await {
            Ok(outcome) => Ok(DirectoryHeadSnapshot {
                head,
                revision: outcome.revision,
                store_id: self.store_id,
            }),
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown) => {
                match self.load_directory_head(head.stream_name).await? {
                    Some(observed) if observed.head == head => Ok(observed),
                    Some(observed) if observed.head.writer_epoch > head.writer_epoch => {
                        Err(StreamError::StaleWriter)
                    }
                    _ => Err(StreamError::WriteStalled),
                }
            }
            Err(error) => Err(kv_error(error)),
        }
    }

    async fn retain_migration_root(&self, head: &StreamDirectoryHead) -> Result<Vec<u8>> {
        let bytes = encode_stream_directory_head(head).map_err(migration_error)?;
        let key = StreamDirectoryRootKey {
            stream_name: head.stream_name,
            identity: head.identity(),
        }
        .to_bytes();
        self.put_directory_immutable(&key, &bytes).await?;
        Ok(bytes)
    }
}
