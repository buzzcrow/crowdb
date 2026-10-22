// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Install a migration fence only after preserving the exact old head bytes.

use bytes::Bytes;
use crowdb_kv_client::BatchOp;
use crowdb_protocol::chunk_stream::{
    decode_stream_head_record, encode_stream_migration_head, legacy_stream_source_id, LegacyManifestView,
    StreamDirectoryControl, StreamHeadRecord, StreamMigrationHead, StreamName, StreamRootIdentity,
};
use crowdb_protocol::key::{BinaryKey, StreamLegacySourceKey, StreamManifestHeadKey};

use super::{kv_error, GetOutcome, KvStreamMetadataStore, ReadMode};
use crate::{Result, StreamError};

mod conversion;
mod finalize;
mod progress;
mod tail;
pub use conversion::StreamMigrationStep;

/// Exact frozen source plus the revision of its bounded migration fence.
#[derive(Clone, Debug)]
pub struct StreamMigrationSnapshot {
    marker: StreamMigrationHead,
    revision: u64,
    source: Bytes,
    store_id: u64,
}

impl StreamMigrationSnapshot {
    #[must_use]
    pub const fn marker(&self) -> &StreamMigrationHead {
        &self.marker
    }
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    #[must_use]
    pub fn source_bytes(&self) -> &Bytes {
        &self.source
    }

    /// # Errors
    /// Rejects a malformed source; the returned view borrows all legacy columns.
    pub fn source_view(&self) -> Result<LegacyManifestView<'_>> {
        LegacyManifestView::decode(&self.source).map_err(migration_error)
    }
}

impl KvStreamMetadataStore {
    /// # Errors
    /// Rejects unknown/wrong-scope heads and missing or corrupt frozen sources.
    pub async fn load_directory_migration(
        &self,
        name: StreamName,
    ) -> Result<Option<StreamMigrationSnapshot>> {
        let Some((value, revision)) = self.migration_head_bytes(name).await? else {
            return Ok(None);
        };
        let Some(StreamHeadRecord::Migrating(marker)) = bounded_head_record(&value)? else {
            return Ok(None);
        };
        self.load_migration_source(name, marker, revision).await.map(Some)
    }

    /// Freeze a legacy head by revision CAS. Conversion may then resume from the
    /// saved source; no indexed root is exposed by this transition.
    ///
    /// # Errors
    /// Rejects stale/different sessions, invalid scope, conflicting saved bytes,
    /// unknown versions and failed or unresolved CAS outcomes.
    pub async fn begin_directory_migration(
        &self,
        name: StreamName,
        writer_epoch: u64,
        owner_session: [u8; 16],
    ) -> Result<StreamMigrationSnapshot> {
        let (source, revision) = self
            .migration_head_bytes(name)
            .await?
            .ok_or_else(|| StreamError::InvalidRequest("legacy stream head is absent".into()))?;
        match bounded_head_record(&source)? {
            Some(StreamHeadRecord::Indexed(_)) => {
                return Err(StreamError::InvalidRequest(
                    "stream directory is already indexed".into(),
                ))
            }
            Some(StreamHeadRecord::Migrating(marker)) => {
                if marker.writer_epoch != writer_epoch || marker.owner_session != owner_session {
                    return Err(StreamError::StaleWriter);
                }
                return self.load_migration_source(name, marker, revision).await;
            }
            None => {}
            Some(StreamHeadRecord::Legacy(_)) => {
                return Err(migration_error("unexpected owned legacy decode"))
            }
        }
        let legacy = LegacyManifestView::decode(&source).map_err(migration_error)?;
        if writer_epoch < legacy.writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        if owner_session == [0; 16] {
            return Err(StreamError::InvalidRequest(
                "migration owner session is empty".into(),
            ));
        }
        let marker = StreamMigrationHead {
            stream_name: name,
            purpose: legacy.purpose,
            metadata_group_id: self.group_id,
            writer_epoch,
            owner_session,
            generation: legacy
                .generation
                .checked_add(1)
                .ok_or_else(|| migration_error("legacy generation overflows"))?,
            legacy: StreamRootIdentity {
                writer_epoch: legacy.writer_epoch,
                generation: legacy.generation,
            },
            source: legacy_stream_source_id(&source),
        };
        marker.validate_source(&source).map_err(migration_error)?;
        let bytes = encode_stream_migration_head(&marker).map_err(migration_error)?;
        let control = Self::directory_control_scope_op(StreamDirectoryControl {
            stream_name: name,
            purpose: marker.purpose,
        })?;
        self.check_migration_control(&control).await?;
        let source_key = StreamLegacySourceKey {
            stream_name: name,
            source: marker.source,
        }
        .to_bytes();
        self.put_directory_immutable(&source_key, &source).await?;
        self.install_migration_fence(marker, revision, bytes, control, source)
            .await
    }

    async fn check_migration_control(&self, control: &BatchOp) -> Result<()> {
        let BatchOp::Put { key, value } = control else {
            unreachable!()
        };
        match self
            .kv
            .get(self.store_id, self.group_id, key, ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        {
            GetOutcome::Found { value: observed, .. } if observed != *value => Err(migration_error(
                "retention control scope differs from legacy stream",
            )),
            _ => Ok(()),
        }
    }

    async fn install_migration_fence(
        &self,
        marker: StreamMigrationHead,
        revision: u64,
        value: Vec<u8>,
        control: BatchOp,
        source: Bytes,
    ) -> Result<StreamMigrationSnapshot> {
        let key = StreamManifestHeadKey {
            stream_name: marker.stream_name,
        }
        .to_bytes();
        let ops = [
            control,
            // Reassert the exact source atomically with the marker: an orphan
            // cleanup that won before this CAS cannot leave a dangling source.
            BatchOp::Put {
                key: Bytes::from(
                    StreamLegacySourceKey {
                        stream_name: marker.stream_name,
                        source: marker.source,
                    }
                    .to_bytes(),
                ),
                value: source,
            },
            BatchOp::Put {
                key: Bytes::copy_from_slice(&key),
                value: Bytes::from(value),
            },
        ];
        match self
            .kv
            .batch_write_cas(self.store_id, self.group_id, &ops, &key, revision)
            .await
        {
            Ok(outcome) => {
                self.load_migration_source(marker.stream_name, marker, outcome.revision)
                    .await
            }
            Err(crowdb_kv_client::Error::CasBusy) => Err(StreamError::Backpressure),
            Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::OutcomeUnknown) => {
                match self.load_directory_migration(marker.stream_name).await? {
                    Some(observed) if observed.marker == marker => Ok(observed),
                    Some(observed) if observed.marker.writer_epoch > marker.writer_epoch => {
                        Err(StreamError::StaleWriter)
                    }
                    _ => Err(StreamError::WriteStalled),
                }
            }
            Err(error) => Err(kv_error(error)),
        }
    }

    async fn migration_head_bytes(&self, name: StreamName) -> Result<Option<(Bytes, u64)>> {
        let key = StreamManifestHeadKey { stream_name: name }.to_bytes();
        match self
            .kv
            .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        {
            GetOutcome::Found { value, revision } => Ok(Some((value, revision))),
            GetOutcome::NotFound => Ok(None),
        }
    }

    async fn load_migration_source(
        &self,
        name: StreamName,
        marker: StreamMigrationHead,
        revision: u64,
    ) -> Result<StreamMigrationSnapshot> {
        if marker.stream_name != name || marker.metadata_group_id != self.group_id {
            return Err(migration_error("migration scope differs from metadata binding"));
        }
        let key = StreamLegacySourceKey {
            stream_name: name,
            source: marker.source,
        }
        .to_bytes();
        let GetOutcome::Found { value, .. } = self
            .kv
            .get(self.store_id, self.group_id, &key, ReadMode::Linearizable, None)
            .await
            .map_err(kv_error)?
        else {
            return Err(StreamError::ReadUnavailable(
                "legacy migration source is absent".into(),
            ));
        };
        marker.validate_source(&value).map_err(migration_error)?;
        Ok(StreamMigrationSnapshot {
            marker,
            revision,
            source: value,
            store_id: self.store_id,
        })
    }
}

fn migration_error(error: impl std::fmt::Display) -> StreamError {
    StreamError::Corruption(format!("legacy stream migration: {error}"))
}

fn bounded_head_record(bytes: &[u8]) -> Result<Option<StreamHeadRecord>> {
    if !bytes.starts_with(b"CSTR") || bytes.starts_with(b"CSTR\x01\x00") {
        LegacyManifestView::decode(bytes).map_err(migration_error)?;
        return Ok(None);
    }
    decode_stream_head_record(bytes)
        .map(Some)
        .map_err(migration_error)
}
