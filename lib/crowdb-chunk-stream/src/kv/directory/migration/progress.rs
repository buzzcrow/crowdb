// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::{
    encode_stream_migration_guard, encode_stream_migration_head, StreamDirectoryControl,
    StreamMigrationGuard, StreamMigrationProgress,
};
use crowdb_protocol::key::{
    BinaryKey, StreamManifestHeadKey, StreamMigrationProgressKey, StreamPublicationGuardKey,
};

use super::{
    kv_error, migration_error, GetOutcome, KvStreamMetadataStore, ReadMode, StreamMigrationSnapshot,
};
use crate::{DirectoryMutation, Result, StreamError};

impl KvStreamMetadataStore {
    /// # Errors
    /// Rejects foreign snapshots, unknown versions and progress outside the frozen source.
    pub async fn load_directory_migration_progress(
        &self,
        snapshot: &StreamMigrationSnapshot,
    ) -> Result<Option<StreamMigrationProgress>> {
        self.validate_migration_snapshot(snapshot)?;
        let key = StreamMigrationProgressKey {
            stream_name: snapshot.marker.stream_name,
            source: snapshot.marker.source,
        }
        .to_bytes();
        let GetOutcome::Found { value, .. } = self
            .kv
            .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        else {
            return Ok(None);
        };
        let progress = StreamMigrationProgress::decode(&value).map_err(migration_error)?;
        if progress.marker != snapshot.marker {
            return Err(StreamError::StaleWriter);
        }
        progress
            .validate_source(&snapshot.source_view()?)
            .map_err(migration_error)?;
        Ok(Some(progress))
    }

    fn validate_migration_snapshot(&self, snapshot: &StreamMigrationSnapshot) -> Result<()> {
        if snapshot.store_id != self.store_id || snapshot.marker.metadata_group_id != self.group_id {
            return Err(StreamError::InvalidRequest(
                "migration snapshot belongs to another metadata store".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn store_migration_progress(
        &self,
        snapshot: &StreamMigrationSnapshot,
        previous: Option<&StreamMigrationProgress>,
        progress: &StreamMigrationProgress,
        mutation: &DirectoryMutation,
    ) -> Result<StreamMigrationSnapshot> {
        self.validate_migration_snapshot(snapshot)?;
        progress
            .validate_source(&snapshot.source_view()?)
            .map_err(migration_error)?;
        if progress.marker != snapshot.marker
            || progress.sequence
                != previous
                    .map_or(Some(1), |old| old.sequence.checked_add(1))
                    .ok_or_else(|| migration_error("conversion sequence overflows"))?
            || previous.is_some_and(|old| {
                old.marker != snapshot.marker
                    || old.logical_tail > progress.logical_tail
                    || old.legacy_root.is_some() && old.legacy_root != progress.legacy_root
            })
            || mutation.base() != previous.and_then(|old| old.root)
            || mutation.index().root() != progress.root
            || mutation.index().stream_name() != snapshot.marker.stream_name
            || mutation.index().purpose() != snapshot.marker.purpose
        {
            return Err(migration_error(
                "conversion progress differs from its fenced path mutation",
            ));
        }
        let guard = StreamMigrationGuard {
            marker: snapshot.marker.clone(),
            sequence: progress.sequence,
            base: mutation.base(),
            root: progress.root,
            legacy_root: progress.legacy_root,
            pages: mutation
                .pages()
                .iter()
                .map(|record| record.reference().id)
                .collect(),
        };
        let (publication_id, bytes) = encode_stream_migration_guard(&guard).map_err(migration_error)?;
        let guard_key = StreamPublicationGuardKey {
            stream_name: snapshot.marker.stream_name,
            publication_id,
        }
        .to_bytes();
        self.protect_directory_records(
            StreamDirectoryControl {
                stream_name: snapshot.marker.stream_name,
                purpose: snapshot.marker.purpose,
            },
            &guard_key,
            &bytes,
        )
        .await?;
        self.store_directory_pages(mutation).await?;
        self.replace_migration_progress(snapshot, progress, guard_key)
            .await
    }

    async fn replace_migration_progress(
        &self,
        snapshot: &StreamMigrationSnapshot,
        progress: &StreamMigrationProgress,
        guard_key: Vec<u8>,
    ) -> Result<StreamMigrationSnapshot> {
        let (key, ops) = migration_progress_ops(snapshot, progress, guard_key)?;
        match self
            .kv
            .batch_write_cas(self.store_id, self.group_id, &ops, &key, snapshot.revision)
            .await
        {
            Ok(outcome) => Ok(StreamMigrationSnapshot {
                marker: snapshot.marker.clone(),
                source: snapshot.source.clone(),
                revision: outcome.revision,
                store_id: self.store_id,
            }),
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown) => {
                let observed = self
                    .load_directory_migration(snapshot.marker.stream_name)
                    .await?
                    .ok_or(StreamError::WriteStalled)?;
                if observed.marker != snapshot.marker {
                    return Err(StreamError::StaleWriter);
                }
                if self.load_directory_migration_progress(&observed).await?.as_ref() == Some(progress) {
                    Ok(observed)
                } else {
                    Err(StreamError::WriteStalled)
                }
            }
            Err(error) => Err(kv_error(error)),
        }
    }
}

fn migration_progress_ops(
    snapshot: &StreamMigrationSnapshot,
    progress: &StreamMigrationProgress,
    guard_key: Vec<u8>,
) -> Result<(Vec<u8>, Vec<BatchOp>)> {
    let key = StreamManifestHeadKey {
        stream_name: snapshot.marker.stream_name,
    }
    .to_bytes();
    let progress_key = StreamMigrationProgressKey {
        stream_name: snapshot.marker.stream_name,
        source: snapshot.marker.source,
    }
    .to_bytes();
    let ops = vec![
        KvStreamMetadataStore::directory_control_scope_op(StreamDirectoryControl {
            stream_name: snapshot.marker.stream_name,
            purpose: snapshot.marker.purpose,
        })?,
        BatchOp::Put {
            key: Bytes::copy_from_slice(&key),
            value: Bytes::from(encode_stream_migration_head(&snapshot.marker).map_err(migration_error)?),
        },
        BatchOp::Put {
            key: Bytes::from(progress_key),
            value: Bytes::from(progress.encode().map_err(migration_error)?),
        },
        BatchOp::Delete {
            key: Bytes::from(guard_key),
        },
    ];
    Ok((key, ops))
}
